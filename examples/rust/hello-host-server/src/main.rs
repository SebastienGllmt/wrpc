use anyhow::Context as _;
use tracing::{debug, error, info, warn};

mod bindings {
    wit_bindgen_wrpc::generate!({
        with: {
            "wrpc-examples:hello/handler": generate,
        }
    });
}

#[derive(Clone, Copy)]
struct Server;

impl bindings::exports::wrpc_examples::hello::handler::Handler<()> for Server {
    async fn hello(&self, _: ()) -> anyhow::Result<String> {
        Ok("hello from Rust".to_string())
    }
}

/**
 * TODO: somehow implement this in a way that it compiles to a WASM Component
 * where the host provides the interface for initializing the server
*/
#[tokio::main(flavor = "current_thread")]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();

    // TODO: this is just testing if calling the transport works
    wrpc_transport_host::foo().await;

    // TODO: rest of the code

    Ok(())
}
