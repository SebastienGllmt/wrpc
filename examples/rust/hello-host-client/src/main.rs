use anyhow::Context as _;
use clap::Parser;

mod bindings {
    wit_bindgen_wrpc::generate!({
        with: {
            "wrpc-examples:hello/handler": generate
        }
    });
}

/**
 * TODO: somehow implement this in a way that it compiles to a WASM Component
 * where the host provides the interface for initializing the client
 * based on ../../../../crates/transport-host/wit/transport.wit
*/
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    let wrpc = wrpc_transport_host::Client::new(/* TODO */);
    let hello = bindings::wrpc_examples::hello::handler::hello(&wrpc, ())
        .await
        .context("failed to invoke `wrpc-examples.hello/handler.hello`")?;
    eprintln!("{hello}");
    Ok(())
}
