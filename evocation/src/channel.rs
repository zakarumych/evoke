use std::{
    future::Future,
    io,
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use alkahest::{Pack, Schema, Unpacked};
use scoped_arena::Scope;

pin_project_lite::pin_project! {
    /// Packet sending future. Produced by `Channel::send`.
    /// If successful, whole packet is sent to recipient.
    /// Otherwise returns error occurred.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Send<'a, C: ?Sized> {
        channel: &'a mut C,
    }
}

impl<'a, C> Future for Send<'a, C>
where
    C: Channel,
{
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let me = self.project();
        match me.channel.poll_send(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
        }
    }
}

pin_project_lite::pin_project! {
    /// Packet receiving future. Produced by `Channel::recv`.
    /// If successful, whole packet is received and unpacked according to schema.
    /// Otherwise returns error occurred.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Recv<'a, 'b, C: ?Sized, S: ?Sized> {
        channel: &'a mut C,
        scope: &'a Scope<'b>,
        schema: PhantomData<S>,
    }
}

impl<'a, C, S> Future for Recv<'a, '_, C, S>
where
    C: Channel,
    S: Schema,
{
    type Output = io::Result<Unpacked<'a, S>>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<Unpacked<'a, S>>> {
        let me = self.project();
        loop {
            match me.channel.poll_recv(cx, S::align(), me.scope) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                Poll::Ready(Ok(packet)) => {
                    let unpacked = alkahest::read::<S>(packet);
                    return Poll::Ready(Ok(unpacked));
                }
            }
        }
    }
}

/// Communication channel with single remote.
pub trait Channel {
    /// Builds packet and sends it to the channel.
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

    /// Polls channel to finish sending operation.
    fn poll_send(&mut self, cx: &mut Context) -> Poll<io::Result<()>>;

    /// Receives packet from the channel.
    /// Resolves to packet size on success.
    /// Otherwise returns error occurred.
    fn poll_recv<'a>(
        &mut self,
        cx: &mut Context,
        align: usize,
        scope: &'a Scope,
    ) -> Poll<io::Result<&'a [u8]>>;

    /// Sends whole typed packed to the channel.
    fn send<'a, S, P, B>(&'a mut self, reliable: bool, pack: P) -> Send<'a, Self>
    where
        S: Schema,
        P: Pack<S>,
    {
        self.start_send(reliable, pack);
        Send { channel: self }
    }

    /// Receives typed packed from the channel.
    /// Resolves to unpacked message on success.
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
