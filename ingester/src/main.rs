#[tokio::main]
async fn main() -> anyhow::Result<()> {
  // connect websocket, consume messages, parse into core::Event,
  // write to DB via db crate, and publish to tokio::sync::broadcast channel.
  Ok(())
}
