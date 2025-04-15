fn main() {
    prost_build::Config::new()
        .compile_protos(&["proto/msg.proto"], &["proto"])
        .unwrap();
    println!("cargo:rerun-if-changed=proto");
}
