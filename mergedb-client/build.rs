fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=../proto/communication.proto");
    tonic_build::compile_protos("../proto/communication.proto")?;
    Ok(())
}