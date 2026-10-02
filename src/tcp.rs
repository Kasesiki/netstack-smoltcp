use bytes::Bytes;
use smol::channel::Receiver;

use crate::stack::IpPacket;

pub struct TcpListener {
    tcp_rx: Receiver<IpPacket<bytes::Bytes>>
}

impl TcpListener {
    pub fn new(tcp_rx: Receiver<IpPacket<Bytes>>) -> Self {
        TcpListener { tcp_rx }
    }
}