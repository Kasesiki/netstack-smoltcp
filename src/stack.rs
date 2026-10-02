use std::{io::{Error, ErrorKind::InvalidInput}, pin::{Pin, pin}, task::{Poll::Ready, ready}};

use bytes::Bytes;
use futures::{Sink, Stream};
use smol::{channel::{self, Receiver, Sender, TryRecvError}};
use smoltcp::wire::{IpProtocol, IpVersion, Ipv4Packet, Ipv6Packet};

use crate::tcp::TcpListener;

pub struct StackBuilder {
    stack_buffer_size: usize,
    tcp_buffer_size: usize,
    udp_buffer_size: usize,
    mtu: u16,
}

impl StackBuilder {
    pub fn new(stack_buffer_size: usize, tcp_buffer_size: usize, udp_buffer_size: usize, mtu: u16) -> Self {
        Self { stack_buffer_size, tcp_buffer_size, udp_buffer_size, mtu }
    }



    pub fn build(self) -> (Stack, TcpListener) {
        let (stack_tx, stack_rx) = channel::bounded(self.stack_buffer_size);
        let (tcp_tx, tcp_rx) = channel::bounded(self.tcp_buffer_size);
        let (udp_tx, udp_rx) = channel::bounded(self.tcp_buffer_size);

        
        (Stack { sink_buf: None, stack_rx, tcp_tx, udp_tx }, TcpListener::new(tcp_rx))
    }
}

impl Default for StackBuilder {
    fn default() -> Self {
        Self { stack_buffer_size: 1024, tcp_buffer_size: 512, udp_buffer_size: 64, mtu: 1500 }
    }
}

pub enum IpPacket<T: AsRef<[u8]>> {
    Ipv4(Ipv4Packet<T>),
    Ipv6(Ipv6Packet<T>),
}

impl<T> IpPacket<T> where T: AsRef<[u8]> {
    pub fn protocol(&self) -> IpProtocol {
        match *self {
            IpPacket::Ipv4(ref packet) => packet.next_header(),
            IpPacket::Ipv6(ref packet) => packet.next_header(),
        }
    }
}

pub struct Stack {
    sink_buf: Option<IpPacket<Bytes>>,
    stack_rx: Receiver<Bytes>,
    tcp_tx: Sender<IpPacket<bytes::Bytes>>,
    udp_tx: Sender<IpPacket<bytes::Bytes>>,
}

impl Stack {
    fn poll_send(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), std::io::Error>> {
        let proto = match unsafe {
            Pin::get_unchecked_mut(self.as_mut()).sink_buf.take()
        } {
            Some(val) => val,
            None => return Ready(Ok(())),
        };

        let tx = match proto.protocol() {
            IpProtocol::Tcp => &self.tcp_tx,
            IpProtocol::Udp => &self.udp_tx,
            _ => unreachable!(),
        };

        if let Err(x) = ready!(pin!(tx.send(proto)).poll(cx)) {
            unsafe {Pin::get_unchecked_mut(self).sink_buf.replace(x.0)};
        };

        Ready(Ok(()))
    } 
}

impl Sink<Bytes> for Stack {
    type Error = std::io::Error;

    fn poll_ready(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        if self.sink_buf.is_some() {
            ready!(self.poll_send(cx))?;
        }
        Ready(Ok(()))
    }

    fn start_send(self: std::pin::Pin<&mut Self>, item: Bytes) -> Result<(), Self::Error> {
        let packet = match IpVersion::of_packet(&item) {
            Ok(_) => IpPacket::Ipv4(Ipv4Packet::new_checked(item).map_err(|err| Error::new(InvalidInput, format!("invalid IP packet: {err}")))?),
            Err(_) => IpPacket::Ipv6(Ipv6Packet::new_checked(item).map_err(|err| Error::new(InvalidInput, format!("invalid IP packet: {err}")))?),
        };
        unsafe { Pin::get_unchecked_mut(self).sink_buf.replace(packet) };
        Ok(())
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.poll_send(cx)
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        self.stack_rx.close();
        Ready(Ok(()))
    }
}

impl Stream for Stack {
    type Item = Bytes;

    fn poll_next(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let message = match self.stack_rx.try_recv() {
            Ok(x) => x,
            Err(e) => {
                if e == TryRecvError::Closed {
                    panic!("{e}")
                } else {
                    return std::task::Poll::Pending;
                }
            }
        };
        Ready(Some(message))
    }
}