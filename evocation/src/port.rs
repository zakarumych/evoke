use std::{
    future::Future,
    io,
    marker::PhantomData,
    num::NonZeroU64,
    pin::Pin,
    task::{Context, Poll},
};

use alkahest::{Pack, Schema, Unpacked};
use scoped_arena::Scope;

pin_project_lite::pin_project! {
    /// Packet sending future. Produced by `Remote::send`.
    /// If successful, whole packet is sent to recipient.
    /// Otherwise returns error occurred.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Send<'a, R: ?Sized> {
        channel: &'a mut R,
    }
}

impl<'a, R> Future for Send<'a, R>
where
    R: Remote,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.project();
        me.channel.poll_send(cx)
    }
}

pin_project_lite::pin_project! {
    /// Packet receiving future. Produced by `Port::recv`.
    /// If successful, whole packet is received and unpacked according to schema.
    /// Otherwise returns error occurred.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Recv<'a, 'b, P: ?Sized, S: ?Sized> {
        channel: &'a mut P,
        scope: &'a Scope<'b>,
        schema: PhantomData<S>,
    }
}

impl<'a, P, S> Future for Recv<'a, '_, P, S>
where
    P: Port,
    S: Schema,
{
    type Output = (RemoteId, io::Result<Unpacked<'a, S>>);

    fn poll(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<(RemoteId, io::Result<Unpacked<'a, S>>)> {
        let me = self.project();
        loop {
            match me.channel.poll_recv(cx, S::align(), me.scope) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready((id, Err(err))) => return Poll::Ready((id, Err(err))),
                Poll::Ready((id, Ok(packet))) => {
                    let unpacked = alkahest::read::<S>(packet);
                    return Poll::Ready((id, Ok(unpacked)));
                }
            }
        }
    }
}

pub trait Remote {
    /// Builds packet and sends it to the remote.
    /// Packet will be sent reliably if `reliable` argument is set to `true`.
    ///
    /// Avoid sending packets reliably when this property is not required.
    ///
    /// # Panics
    ///
    /// This method may panic if previous send is not complete.
    /// To ensure send operation is complete, call `Channel::poll_send` until it returns `Poll::Ready`.
    fn start_send<S, P>(&mut self, reliable: bool, pack: P)
    where
        S: Schema,
        P: Pack<S>;

    /// Polls remote to finish sending operation.
    fn poll_send(&mut self, cx: &mut Context) -> Poll<io::Result<()>>;

    fn send<'a, S, P>(&'a mut self, reliable: bool, pack: P) -> Send<'a, Self>
    where
        S: Schema,
        P: Pack<S>,
    {
        self.start_send(reliable, pack);
        Send { channel: self }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct RemoteId(pub NonZeroU64);

/// Communication channel with multiple connected remotes.
pub trait Port {
    /// Connected remote.
    type Remote: Remote;

    /// Accepts remote connection.
    /// Resolves to `Remote` value on success.
    /// Otherwise returns error occurred.
    fn poll_accept(&mut self, cx: &mut Context) -> Poll<io::Result<Option<Self::Remote>>>;

    /// Receives packet from the port.
    /// Resolves to packet and remote id on success.
    /// Otherwise returns error occurred.
    fn poll_recv<'a>(
        &mut self,
        cx: &mut Context,
        align: usize,
        scope: &'a Scope,
    ) -> Poll<(RemoteId, io::Result<&'a [u8]>)>;

    /// Receives typed packed from the port.
    /// Resolves to unpacked message and remote id on success.
    /// Otherwise returns error occurred.
    fn recv<'a, 'b, S, P>(&'a mut self, scope: &'a Scope<'b>) -> Recv<'a, 'b, Self, S>
    where
        S: Schema,
        P: Pack<S>,
    {
        Recv {
            channel: self,
            scope,
            schema: PhantomData::<S>,
        }
    }
}
