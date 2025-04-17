use percolator_proto::message;
use prost::Message;
fn main() {
    let msg = message::GetResponse {
        ..Default::default()
    };
    println!("Hello, world: {}!", msg.encoded_len());
}
