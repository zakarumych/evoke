use std::{
    alloc::Layout,
    convert::TryFrom,
    io::{self, Error, ErrorKind},
    mem::size_of,
    num::NonZeroU64,
    pin::Pin,
    ptr::copy_nonoverlapping,
    task::{Context, Poll},
};

use alkahest::{Pack, Schema};
use bytemuck::{Pod, Zeroable};
use futures::task::noop_waker_ref;
use hashbrown::HashMap;
use scoped_arena::Scope;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpListener, TcpStream, ToSocketAddrs,
    },
};

use crate::{
    channel::Channel,
    port::{Port, Remote, RemoteId},
};

const MAX_PACKET_SIZE: usize = 1 << 18;

struct RecvCtx {
    buf: Box<[u8]>,
    buf_off: usize,
    buf_len: usize,
}

impl RecvCtx {
    fn new() -> Self {
        RecvCtx {
            buf: vec![0u8; MAX_PACKET_SIZE].into_boxed_slice(),
            buf_off: 0,
            buf_len: 0,
        }
    }

    fn poll_recv<'a>(
        &mut self,
        cx: &mut Context,
        mut read: Pin<&mut impl AsyncRead>,
        align: usize,
        scope: &'a Scope,
    ) -> Poll<io::Result<&'a [u8]>> {
        // Get header
        let header = loop {
            if self.buf_len < size_of::<TcpHeader>() {
                debug_assert!(size_of::<TcpHeader>() <= self.buf.len());

                if self.buf_off > self.buf.len() - size_of::<TcpHeader>() {
                    debug_assert!(self.buf_len < self.buf_off);
                    let (head, tail) = self.buf.split_at_mut(self.buf_len);
                    head.copy_from_slice(&tail[self.buf_off - self.buf_len..self.buf_off]);
                    self.buf_off = 0;
                }

                let mut readbuf = ReadBuf::new(&mut self.buf[self.buf_off + self.buf_len..]);
                let result = read.as_mut().poll_read(cx, &mut readbuf);

                match result {
                    Poll::Ready(Ok(())) => {
                        if readbuf.filled().is_empty() {
                            return Poll::Ready(Err(ErrorKind::ConnectionAborted.into()));
                        } else {
                            self.buf_len += readbuf.filled().len();
                        }
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                }

                continue;
            } else {
                let mut header = TcpHeader { magic: 0, size: 0 };

                bytemuck::bytes_of_mut(&mut header)
                    .copy_from_slice(&self.buf[self.buf_off..][..size_of::<TcpHeader>()]);

                // Check magic value
                if u32::from_le(header.magic) != MAGIC {
                    self.buf_off += 1;
                    continue;
                }

                break header;
            }
        };

        let size = header.size as usize;

        // Get payload
        let payload = loop {
            if self.buf_len - size_of::<TcpHeader>() < size {
                assert!(size_of::<TcpHeader>() + size < self.buf.len());

                if self.buf_off > self.buf.len() - size_of::<TcpHeader>() - size {
                    // Rotate buf
                    if self.buf_len < self.buf_off {
                        let (head, tail) = self.buf.split_at_mut(self.buf_len);
                        head.copy_from_slice(&tail[self.buf_off - self.buf_len..self.buf_off]);
                    } else {
                        unsafe {
                            std::ptr::copy(
                                self.buf.as_ptr().add(self.buf_off),
                                self.buf.as_mut_ptr(),
                                self.buf_len,
                            );
                        }
                    }
                    self.buf_off = 0;
                }

                let mut readbuf = ReadBuf::new(&mut self.buf[self.buf_off + self.buf_len..]);
                let result = read.as_mut().poll_read(cx, &mut readbuf);

                match result {
                    Poll::Ready(Ok(())) => {
                        if readbuf.filled().is_empty() {
                            return Poll::Ready(Err(ErrorKind::ConnectionAborted.into()));
                        } else {
                            self.buf_len += readbuf.filled().len();
                        }
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                    Poll::Pending => return Poll::Pending,
                }

                continue;
            } else {
                let buf = scope.alloc(Layout::from_size_align(size, align).unwrap());

                let buf = unsafe {
                    std::ptr::copy_nonoverlapping(
                        self.buf[self.buf_off + size_of::<TcpHeader>()..].as_ptr(),
                        buf.as_mut_ptr() as *mut u8,
                        size,
                    );
                    std::slice::from_raw_parts(buf.as_mut_ptr() as *mut u8, size)
                };

                break buf;
            }
        };

        self.buf_off += size_of::<TcpHeader>() + size;
        self.buf_len -= size_of::<TcpHeader>() + size;

        Poll::Ready(Ok(payload))
    }
}

struct SendCtx {
    buf: Box<[u8]>,
    buf_off: usize,
    buf_len: usize,
}

impl SendCtx {
    fn new() -> Self {
        SendCtx {
            buf: vec![0; MAX_PACKET_SIZE].into_boxed_slice(),
            buf_off: 0,
            buf_len: 0,
        }
    }

    fn start_send<'a, S, P>(&mut self, pack: P)
    where
        S: Schema,
        P: Pack<S>,
    {
        assert!(
            self.buf_off == self.buf_len,
            "Last packet is not sent completely yet"
        );

        let size = alkahest::write(&mut self.buf[size_of::<TcpHeader>()..], pack);

        if u32::try_from(size).is_err() {
            panic!("Packet is too large");
        }

        let header = TcpHeader {
            magic: MAGIC.to_le(),
            size: (size as u32).to_le(),
        };

        self.buf[..size_of::<TcpHeader>()].copy_from_slice(bytemuck::bytes_of(&header));
        self.buf_off = 0;
        self.buf_len = size + size_of::<TcpHeader>();
    }

    fn poll_send(
        &mut self,
        cx: &mut Context,
        mut write: Pin<&mut impl AsyncWrite>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self.buf_off == self.buf_len {
                return Poll::Ready(Ok(()));
            }
            match write
                .as_mut()
                .poll_write(cx, &self.buf[self.buf_off..self.buf_len])
            {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(n)) => {
                    self.buf_off += n;
                }
            }
        }
    }
}

pub struct TcpChannel {
    stream: TcpStream,
    send_ctx: SendCtx,
    recv_ctx: RecvCtx,
}

impl TcpChannel {
    pub fn new(stream: TcpStream) -> Self {
        TcpChannel {
            stream,
            send_ctx: SendCtx::new(),
            recv_ctx: RecvCtx::new(),
        }
    }

    pub async fn connect(addrs: impl ToSocketAddrs) -> Result<Self, Error> {
        let stream = TcpStream::connect(addrs).await?;
        Ok(TcpChannel::new(stream))
    }
}

impl Channel for TcpChannel {
    fn start_send<S, P>(&mut self, _reliable: bool, pack: P)
    where
        S: Schema,
        P: Pack<S>,
    {
        self.send_ctx.start_send::<S, P>(pack);
    }

    fn poll_send(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        self.send_ctx.poll_send(cx, Pin::new(&mut self.stream))
    }

    fn poll_recv<'a>(
        &mut self,
        cx: &mut Context,
        align: usize,
        scope: &'a Scope,
    ) -> Poll<io::Result<&'a [u8]>> {
        self.recv_ctx
            .poll_recv(cx, Pin::new(&mut self.stream), align, scope)
    }
}

pub struct TcpRemote {
    stream: OwnedWriteHalf,
    send_ctx: SendCtx,
}

impl Remote for TcpRemote {
    fn start_send<S, P>(&mut self, _reliable: bool, pack: P)
    where
        S: Schema,
        P: Pack<S>,
    {
        self.send_ctx.start_send(pack);
    }

    fn poll_send(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        self.send_ctx.poll_send(cx, Pin::new(&mut self.stream))
    }
}

struct TcpPortRemote {
    stream: OwnedReadHalf,
    recv_ctx: RecvCtx,
}

pub struct TcpPort {
    listener: TcpListener,
    streams: HashMap<NonZeroU64, TcpPortRemote>,
    next_id: u64,
}

impl TcpPort {
    pub fn new(listener: TcpListener) -> Self {
        TcpPort {
            listener,
            streams: HashMap::new(),
            next_id: 1,
        }
    }

    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(TcpPort::new(TcpListener::bind(addr).await?))
    }
}

impl Port for TcpPort {
    type Remote = TcpRemote;

    fn poll_accept(&mut self, _cx: &mut Context) -> Poll<io::Result<Option<TcpRemote>>> {
        let next_id = NonZeroU64::new(self.next_id).expect("u64 overflow");

        let waker = noop_waker_ref();
        match self.listener.poll_accept(&mut Context::from_waker(waker)) {
            Poll::Pending => Poll::Ready(Ok(None)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok((stream, addr))) => {
                let (read, write) = stream.into_split();
                tracing::info!("Accepted TCP connection from {}", addr);

                let old = self.streams.insert(
                    next_id,
                    TcpPortRemote {
                        stream: read,
                        recv_ctx: RecvCtx::new(),
                    },
                );
                debug_assert!(old.is_none());

                self.next_id += 1;
                Poll::Ready(Ok(Some(TcpRemote {
                    stream: write,
                    send_ctx: SendCtx::new(),
                })))
            }
        }
    }

    fn poll_recv<'a>(
        &mut self,
        cx: &mut Context,
        align: usize,
        scope: &'a Scope,
    ) -> Poll<(RemoteId, io::Result<&'a [u8]>)> {
        for (&id, stream) in &mut self.streams {
            let poll = stream
                .recv_ctx
                .poll_recv(cx, Pin::new(&mut stream.stream), align, scope);

            match poll {
                Poll::Pending => continue,
                Poll::Ready(Err(err)) => return Poll::Ready((RemoteId(id), Err(err))),
                Poll::Ready(Ok(packet)) => return Poll::Ready((RemoteId(id), Ok(packet))),
            }
        }
        Poll::Pending
    }
}

const MAGIC: u32 = u32::from_le_bytes(*b"astr");

#[derive(Copy, Clone, Zeroable, Pod)]
#[repr(C)]
struct TcpHeader {
    magic: u32,
    size: u32,
}
