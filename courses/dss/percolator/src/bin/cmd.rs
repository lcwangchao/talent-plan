use percolator_proto::message;
fn main() {
    let msg = message::TimestampResponse { timestamp: 123 };
    println!("Hello, world: {}!", msg.timestamp);
}
