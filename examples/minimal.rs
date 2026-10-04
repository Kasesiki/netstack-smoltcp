use netstack_smoltcp::stack::StackBuilder;

fn main() {
    let (_stack, mut _tcplistener) = StackBuilder::default().build();
}