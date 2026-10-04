
use bytes::Bytes;
use futures::{channel::mpsc::{Receiver, Sender, UnboundedSender}};
use smoltcp::phy::{Device, RxToken, TxToken};


pub struct VirtualDevice {
    /// 接收过滤后的frame
    in_buf: Receiver<Bytes>,

    /// 将数据发送到tun
    out_buf: UnboundedSender<Bytes>,
}

impl VirtualDevice {
    pub fn new(
        iface_egress_tx: UnboundedSender<Bytes>,
    ) -> (Self, Sender<Bytes>) {
        
        let (iface_ingress_tx, iface_ingress_rx) = futures::channel::mpsc::channel(10);
        (
            Self {
                in_buf: iface_ingress_rx,
                out_buf: iface_egress_tx,
            },
            iface_ingress_tx,
        )
    }
}

impl Device for VirtualDevice {
    type RxToken<'a> = VirtualRxToken;
    type TxToken<'a> = VirtualTxToken;

    fn receive(&mut self, _timestamp: smoltcp::time::Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        
        let Ok(buf) = self.in_buf.try_recv() else {return None};

        Some((VirtualRxToken { buffer: buf }, VirtualTxToken { permit: self.out_buf.clone() }))
    }

    fn transmit(&mut self, _timestamp: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        todo!()
    }

    fn capabilities(&self) -> smoltcp::phy::DeviceCapabilities {
        todo!()
    }
}

pub struct VirtualRxToken {
    buffer: Bytes,
}

impl RxToken for VirtualRxToken {
    fn consume<R, F>(self, f: F) -> R
    where
        F: FnOnce(&[u8]) -> R,
    {
        f(&self.buffer[..])
    }
}

pub struct VirtualTxToken {
    permit: UnboundedSender<Bytes>,
}

impl TxToken for VirtualTxToken {
    fn consume<R, F>(self, len: usize, f: F) -> R
    where
        F: FnOnce(&mut [u8]) -> R,
    {
        let mut buffer = vec![0u8; len];
        let result = f(&mut buffer);

        let _ = self.permit.unbounded_send(buffer.into());
        result
    }
}

