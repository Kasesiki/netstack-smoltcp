use std::{net::SocketAddr, pin::pin, task::ready};

use bytes::Bytes;
use futures::{Stream, channel::mpsc::Receiver};
use smoltcp::{socket::tcp::SocketBuffer, time::Duration, wire::TcpPacket};

use crate::stack::IpPacket;

pub struct TcpListener {
    tcp_rx: Receiver<IpPacket<bytes::Bytes>>,
    tcp_buffer_size: usize,
}

impl TcpListener {
    pub fn new(tcp_rx: Receiver<IpPacket<Bytes>>, tcp_buffer_size: usize) -> Self {
        TcpListener { tcp_rx, tcp_buffer_size }
    }
}

impl Stream for TcpListener {
    type Item = (SocketAddr, SocketAddr);

    fn poll_next(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Option<Self::Item>> {
        let Ok(a) = ready!(pin!(self.tcp_rx.recv()).poll(cx)) else { panic!() };

        let dst_addr = a.dst_addr() ;

        let Ok(packet) = TcpPacket::new_checked(a.take()) else { panic!() };

        if packet.syn() && !packet.ack() {
            let mut socket = smoltcp::socket::tcp::Socket::new(SocketBuffer::new(vec![0u8; self.tcp_buffer_size]), SocketBuffer::new(vec![0u8; self.tcp_buffer_size]));

            socket.set_keep_alive(Some(Duration::from_secs(28)));
            socket.set_timeout(Some(Duration::from_secs(7200)));

            socket.listen((dst_addr, packet.dst_port())).unwrap();
        }
        todo!()
    }
}