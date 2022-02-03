use core::slice;
use std::{
    alloc::Layout,
    convert::TryFrom,
    io::{self, Error, ErrorKind},
    mem::{size_of, swap, MaybeUninit},
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroU64,
    sync::Arc,
    task::{Context, Poll},
};

use alkahest::{Pack, Pod, Schema, Zeroable};
use flume::{Receiver, Sender};
use hashbrown::{hash_map::Entry, HashMap};
use scoped_arena::Scope;
use tokio::{
    io::ReadBuf,
    net::{lookup_host, ToSocketAddrs, UdpSocket},
};

use crate::{
    channel::Channel,
    port::{Port, Remote, RemoteId},
};

const MAX_PACKET_SIZE: usize = 1 << 16;

const RESEND_LENGTH: u8 = u64::BITS as u8;
const RESEND_OFFSET: u8 = RESEND_LENGTH / 2;

struct Packet {
    buf: Box<[u8]>,
    buf_len: usize,
    seq: u64,
    sent: bool,
}

struct SendCtx {
    last_packets: [Packet; RESEND_LENGTH as usize],
    pending: Packet,
    pending_reliable: bool,
    last_packets_offset: u8,
    seq: u64,
}

impl SendCtx {
    fn new() -> Self {
        SendCtx {
            last_packets_offset: 0,
            last_packets: [(); RESEND_LENGTH as usize].map(|()| Packet {
                buf: Box::new([]),
                buf_len: 0,
                seq: 0,
                sent: true,
            }),
            pending: Packet {
                buf: Box::new([]),
                buf_len: 0,
                seq: 0,
                sent: true,
            },
            pending_reliable: false,
            seq: 1,
        }
    }

    fn start_send<'a, S, P>(&mut self, reliable: bool, pack: P) -> io::Result<()>
    where
        S: Schema,
        P: Pack<S>,
    {
        assert!(self.pending.sent, "Last packet is not sent completely yet");

        if reliable && !self.last_packets[self.last_packets_offset as usize].sent {
            // Too many packets were not sent successfully.
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to deliver reliable message",
            ));
        }

        if self.pending.buf.len() < MAX_PACKET_SIZE {
            self.pending.buf = vec![0; MAX_PACKET_SIZE].into_boxed_slice();
        }

        let size = alkahest::write(&mut self.pending.buf[size_of::<UdpHeader>()..], pack);

        if u32::try_from(size).is_err() {
            panic!("Packet is too large");
        }

        let seq = if reliable { self.seq } else { u64::MAX };

        let header = UdpHeader { seq: seq.to_le() };
        self.pending.buf[..size_of::<UdpHeader>()].copy_from_slice(bytemuck::bytes_of(&header));
        self.pending.buf_len = size_of::<UdpHeader>() + size;
        self.pending.seq = seq;
        self.pending.sent = false;
        self.pending_reliable = reliable;
        Ok(())
    }

    fn poll_send_impl(&mut self, poll: Poll<io::Result<usize>>) -> Poll<io::Result<()>> {
        match poll {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(n)) => {
                debug_assert_eq!(
                    n, self.pending.buf_len,
                    "UdpSocket must send whole packet at once"
                );
                debug_assert!(!self.pending.sent);

                if self.pending_reliable {
                    swap(
                        &mut self.pending,
                        &mut self.last_packets[self.last_packets_offset as usize],
                    );

                    self.last_packets_offset += 1;
                    self.last_packets_offset %= RESEND_LENGTH;

                    debug_assert!(self.pending.sent);

                    // Consider resending message.
                    let resend_index = (self.last_packets_offset + RESEND_OFFSET) % RESEND_LENGTH;
                    if !self.last_packets[resend_index as usize].sent {
                        if !self.last_packets[self.last_packets_offset as usize].sent {
                            // Too many packets were not sent successfully.
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Failed to deliver reliable message",
                            )));
                        }

                        swap(
                            &mut self.pending,
                            &mut self.last_packets[resend_index as usize],
                        );
                    } else {
                        // Next seq.
                        self.seq += 1;
                        if self.seq == 0 || self.seq == u64::MAX {
                            // 0 is reserved for ACK.
                            // MAX is reserved for non-reliable.
                            self.seq = 1;
                        }
                    }
                } else {
                    self.pending.sent = true;
                }
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_send_to(
        &mut self,
        cx: &mut Context,
        socket: &UdpSocket,
        target: SocketAddr,
    ) -> Poll<io::Result<()>> {
        while !self.pending.sent {
            let poll = socket.poll_send_to(cx, &self.pending.buf[..self.pending.buf_len], target);
            match self.poll_send_impl(poll) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => continue,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn poll_send(&mut self, cx: &mut Context, socket: &UdpSocket) -> Poll<io::Result<()>> {
        while !self.pending.sent {
            let poll = socket.poll_send(cx, &self.pending.buf[..self.pending.buf_len]);
            match self.poll_send_impl(poll) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => continue,
            }
        }
        Poll::Ready(Ok(()))
    }

    fn on_ack(&mut self, seq: u64, mask: u64) {
        for packet in &mut self.last_packets {
            if !packet.sent {
                if packet.seq == seq {
                    packet.sent = true;
                } else if packet.seq < seq && seq - packet.seq < 64 {
                    let bit = 1 << seq - packet.seq - 1;
                    if mask & bit == bit {
                        packet.sent = true;
                    }
                }
            }
        }
    }
}

struct RecvCtx {
    seq: u64,
    mask: u64,
    pending_ack: bool,
}

impl RecvCtx {
    fn new() -> Self {
        RecvCtx {
            seq: 0,
            mask: 0,
            pending_ack: false,
        }
    }

    fn poll_send_ack(&mut self, cx: &mut Context, socket: &UdpSocket) -> Poll<io::Result<()>> {
        if !self.pending_ack {
            Poll::Ready(Ok(()))
        } else {
            let ack = UdpAck {
                zero: 0,
                seq: self.seq,
                mask: self.mask,
            };
            match socket.poll_send(cx, bytemuck::bytes_of(&ack)) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(n)) => {
                    debug_assert_eq!(n, size_of::<UdpAck>());
                    self.pending_ack = false;
                    Poll::Ready(Ok(()))
                }
            }
        }
    }

    fn poll_send_ack_to(
        &mut self,
        cx: &mut Context,
        socket: &UdpSocket,
        target: SocketAddr,
    ) -> Poll<io::Result<()>> {
        if !self.pending_ack {
            Poll::Ready(Ok(()))
        } else {
            let ack = UdpAck {
                zero: 0,
                seq: self.seq,
                mask: self.mask,
            };
            match socket.poll_send_to(cx, bytemuck::bytes_of(&ack), target) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(n)) => {
                    debug_assert_eq!(n, size_of::<UdpAck>());
                    self.pending_ack = false;
                    Poll::Ready(Ok(()))
                }
            }
        }
    }

    fn poll_recv<'a>(
        &mut self,
        cx: &mut Context,
        socket: &UdpSocket,
        align: usize,
        scope: &'a Scope,
        mut on_ack: impl FnMut(u64, u64),
    ) -> Poll<io::Result<&'a [u8]>> {
        let buf = make_buf(align, scope);

        loop {
            let mut readbuf = ReadBuf::uninit(buf);
            match socket.poll_recv(cx, &mut readbuf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(())) => {
                    let len = readbuf.filled().len();
                    let packet =
                        unsafe { slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, len) };

                    match self.recv_impl(packet, align, &mut on_ack) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                        Poll::Ready(Ok(Some(payload))) => return Poll::Ready(Ok(payload)),
                        Poll::Ready(Ok(None)) => continue,
                    }
                }
            }
        }
    }

    fn recv_impl<'a>(
        &mut self,
        buf: &'a [u8],
        align: usize,
        mut on_ack: impl FnMut(u64, u64),
    ) -> Poll<io::Result<Option<&'a [u8]>>> {
        if buf.len() < size_of::<UdpHeader>() {
            tracing::warn!("Too small datagram received");
            return Poll::Ready(Ok(None));
        }

        let mut header = UdpHeader { seq: 0 };
        bytemuck::bytes_of_mut(&mut header).copy_from_slice(&buf[..size_of::<UdpHeader>()]);

        match header.seq {
            0 => {
                // ACK packet.
                if buf.len() < size_of::<UdpAck>() {
                    tracing::warn!("Too small ACK datagram received");
                    return Poll::Ready(Ok(None));
                }

                let mut ack = UdpAck {
                    zero: 0,
                    seq: 0,
                    mask: 0,
                };

                bytemuck::bytes_of_mut(&mut ack).copy_from_slice(&buf[..size_of::<UdpAck>()]);

                if ack.seq == 0 || ack.seq == u64::MAX {
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Invalid ACK.SEQ: {}", ack.seq),
                    )));
                }

                on_ack(ack.seq, ack.mask);
                return Poll::Ready(Ok(None));
            }
            _ => {
                // Received message. Yey!
                if header.seq != u64::MAX {
                    // Reliable message.
                    if header.seq == self.seq {
                        // Duplicate
                        return Poll::Ready(Ok(None));
                    } else if header.seq > self.seq {
                        // Fresh message received.
                        if header.seq - self.seq < 64 {
                            let mask = u64::MAX << header.seq - self.seq;
                            if self.mask & mask != mask {
                                return Poll::Ready(Err(io::Error::new(
                                    io::ErrorKind::Other,
                                    "Older reliable messages are not received",
                                )));
                            }
                        } else {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::Other,
                                "Older reliable messages are not received",
                            )));
                        }
                        self.mask <<= header.seq - self.seq;
                        self.seq = header.seq;
                    } else {
                        // Older message received.
                        let back = self.seq - header.seq - 1;
                        if back > 64 {
                            // Old duplicate.
                            return Poll::Ready(Ok(None));
                        }

                        let bit = 1 << back;
                        if self.mask & bit == bit {
                            // Duplicate.
                            return Poll::Ready(Ok(None));
                        }
                        self.mask |= bit;
                    }
                }

                let payload = &buf[size_of::<UdpHeader>()..];
                debug_assert_eq!(payload.as_ptr() as usize % align, 0);
                Poll::Ready(Ok(Some(payload)))
            }
        }
    }
}

pub struct UdpChannel {
    socket: UdpSocket,
    target: SocketAddr,
    send_ctx: SendCtx,
    recv_ctx: RecvCtx,
}

impl UdpChannel {
    pub fn new(socket: UdpSocket, target: SocketAddr) -> Self {
        UdpChannel {
            socket,
            target,
            send_ctx: SendCtx::new(),
            recv_ctx: RecvCtx::new(),
        }
    }

    pub async fn connect(addrs: impl ToSocketAddrs) -> io::Result<Self> {
        let addrs = lookup_host(addrs).await?;

        let mut last_err = None;
        let socket = UdpSocket::bind((Ipv4Addr::UNSPECIFIED, 0)).await?;

        for addr in addrs {
            match connect_addr(&socket, addr).await {
                Ok(()) => return Ok(UdpChannel::new(socket, addr)),
                Err(e) => last_err = Some(e),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            Error::new(ErrorKind::InvalidInput, "could not resolve to any address")
        }))
    }
}

async fn connect_addr(socket: &UdpSocket, addr: SocketAddr) -> io::Result<()> {
    let mut handshake = UdpHandshake {
        magic: MAGIC.to_le(),
    };

    socket.send_to(bytemuck::bytes_of(&handshake), addr).await?;
    handshake.magic = 0;
    socket.recv(bytemuck::bytes_of_mut(&mut handshake)).await?;

    if handshake.magic == MAGIC.to_le() {
        socket.connect(addr).await?;
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::Other,
            "Invalid UDP channel handshake",
        ))
    }
}

impl Channel for UdpChannel {
    fn start_send<S, P>(&mut self, reliable: bool, pack: P)
    where
        S: Schema,
        P: Pack<S>,
    {
        self.send_ctx.start_send(reliable, pack);
    }

    fn poll_send(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        self.send_ctx.poll_send(cx, &self.socket)
    }

    fn poll_recv<'a>(
        &mut self,
        cx: &mut Context,
        align: usize,
        scope: &'a Scope,
    ) -> Poll<io::Result<&'a [u8]>> {
        match self.recv_ctx.poll_send_ack(cx, &self.socket) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => {}
        }

        self.recv_ctx
            .poll_recv(cx, &self.socket, align, scope, |seq, mask| {
                self.send_ctx.on_ack(seq, mask)
            })
    }
}

pub struct UdpRemote {
    socket: Arc<UdpSocket>,
    send_ctx: SendCtx,
    rx: Receiver<(u64, u64)>,
}

impl Remote for UdpRemote {
    fn start_send<S, P>(&mut self, reliable: bool, pack: P)
    where
        S: Schema,
        P: Pack<S>,
    {
        self.send_ctx.start_send(reliable, pack);
    }

    fn poll_send(&mut self, cx: &mut Context) -> Poll<io::Result<()>> {
        self.send_ctx.poll_send(cx, &self.socket)
    }
}

struct UdpPortRemote {
    id: RemoteId,
    recv_ctx: RecvCtx,
    tx: Sender<(u64, u64)>,
}

pub struct UdpPort {
    remotes: HashMap<SocketAddr, UdpPortRemote>,
    new_remotes: Vec<SocketAddr>,
    socket: Arc<UdpSocket>,
    next_id: u64,
    error: Option<io::Error>,
}

impl UdpPort {
    pub fn new(socket: UdpSocket) -> Self {
        UdpPort {
            remotes: HashMap::new(),
            new_remotes: Vec::new(),
            socket: Arc::new(socket),
            next_id: 1,
            error: None,
        }
    }

    pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        Ok(UdpPort::new(UdpSocket::bind(addr).await?))
    }
}

impl Port for UdpPort {
    type Remote = UdpRemote;

    fn poll_accept(&mut self, cx: &mut Context) -> Poll<io::Result<Option<UdpRemote>>> {
        if let Some(err) = self.error.take() {
            return Poll::Ready(Err(err));
        }

        let handshake = UdpHandshake {
            magic: MAGIC.to_le(),
        };

        while let Some(addr) = self.new_remotes.pop() {
            match self
                .socket
                .poll_send_to(cx, bytemuck::bytes_of(&handshake), addr)
            {
                Poll::Pending => {
                    self.new_remotes.push(addr);
                    return Poll::Pending;
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(n)) => {
                    debug_assert_eq!(n, size_of::<UdpHandshake>());

                    let id = RemoteId(NonZeroU64::new(self.next_id).unwrap());
                    self.next_id += 1;

                    let (tx, rx) = flume::unbounded();

                    self.remotes.insert(
                        addr,
                        UdpPortRemote {
                            id,
                            recv_ctx: RecvCtx::new(),
                            tx,
                        },
                    );

                    let remote = UdpRemote {
                        socket: self.socket.clone(),
                        send_ctx: SendCtx::new(),
                        rx,
                    };

                    return Poll::Ready(Ok(Some(remote)));
                }
            }
        }

        Poll::Ready(Ok(None))
    }

    fn poll_recv<'a>(
        &mut self,
        cx: &mut Context,
        align: usize,
        scope: &'a Scope,
    ) -> Poll<(RemoteId, io::Result<&'a [u8]>)> {
        if self.error.is_some() {
            return Poll::Pending;
        }

        for (&target, remote) in &mut self.remotes {
            match remote.recv_ctx.poll_send_ack_to(cx, &self.socket, target) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready((remote.id, Err(err))),
                Poll::Ready(Ok(())) => {}
            }
        }

        let buf = make_buf(align, scope);
        loop {
            let mut readbuf = ReadBuf::uninit(buf);
            match self.socket.poll_recv_from(cx, &mut readbuf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => {
                    self.error = Some(err);
                    return Poll::Pending;
                }
                Poll::Ready(Ok(addr)) => {
                    let len = readbuf.filled().len();
                    let packet =
                        unsafe { slice::from_raw_parts_mut(buf.as_mut_ptr() as *mut u8, len) };

                    match self.remotes.get_mut(&addr) {
                        None => {
                            if packet.len() != size_of::<UdpHandshake>() {
                                // First message must be `UdpHandshake`.
                                continue;
                            }

                            let mut handshake = UdpHandshake { magic: 0 };
                            bytemuck::bytes_of_mut(&mut handshake).copy_from_slice(packet);

                            if handshake.magic != MAGIC.to_le() {
                                // First message must be `UdpHandshake { magic: MAGIC.to_le() }`.
                                continue;
                            }

                            self.new_remotes.push(addr);
                        }
                        Some(remote) => {
                            match remote.recv_ctx.recv_impl(packet, align, |seq, mask| {
                                remote.tx.send((seq, mask));
                            }) {
                                Poll::Pending => return Poll::Pending,
                                Poll::Ready(Err(err)) => return Poll::Ready((remote.id, Err(err))),
                                Poll::Ready(Ok(Some(payload))) => {
                                    return Poll::Ready((remote.id, Ok(payload)))
                                }
                                Poll::Ready(Ok(None)) => continue,
                            }
                        }
                    }
                }
            }
        }
    }
}

const MAGIC: u32 = u32::from_le_bytes(*b"astr");

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct UdpHandshake {
    magic: u32,
}

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct UdpHeader {
    seq: u64,
}

#[derive(Clone, Copy, Zeroable, Pod)]
#[repr(C)]
struct UdpAck {
    zero: u64,
    seq: u64,
    mask: u64,
}

fn make_buf<'a>(align: usize, scope: &'a Scope) -> &'a mut [MaybeUninit<u8>] {
    assert!(align <= 1024);

    let buf = scope.alloc(Layout::from_size_align(MAX_PACKET_SIZE + align, align).unwrap());

    // Offset buffer to ensure payload is aligned.
    let start = buf.as_ptr() as usize;
    let aligned_payload_start = (start + size_of::<UdpHeader>() + align - 1) & !(align - 1);
    let header_start = aligned_payload_start - size_of::<UdpHeader>();
    let header_offset = header_start - start;
    &mut buf[header_offset..]
}
