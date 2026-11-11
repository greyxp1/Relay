mod call;
mod capture;
mod dave;
mod playback;
mod transport;

use std::{net::Ipv4Addr, sync::Arc};
use tokio::{
    net::TcpStream,
    sync::{OwnedSemaphorePermit, Semaphore},
};
use tokio_tungstenite::{
    accept_hdr_async_with_config,
    tungstenite::{
        handshake::server::{Request, Response},
        http::{header, StatusCode},
        protocol::WebSocketConfig,
    },
    WebSocketStream,
};

pub type WebSocket = WebSocketStream<TcpStream>;

const PORT: u16 = 38446;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let started = std::time::Instant::now();
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args == ["--help"] {
        println!("relay");
        return Ok(());
    }
    if !args.is_empty() {
        return Err("Unknown argument".into());
    }
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, PORT)).await?;
    let clients = Arc::new(Semaphore::new(16));
    tokio::task::LocalSet::new()
        .run_until(async move {
            eprintln!(
                "ready in {:.1} ms on 127.0.0.1:{PORT}",
                started.elapsed().as_secs_f64() * 1000.0
            );
            loop {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => return Ok::<_, std::io::Error>(()),
                    connection = listener.accept() => {
                        let (stream, _) = match connection {
                            Ok(connection) => connection,
                            Err(_) => {
                                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                                continue;
                            }
                        };
                        let clients = clients.clone();
                        tokio::task::spawn_local(serve(stream, clients));
                    }
                }
            }
        })
        .await?;
    Ok(())
}

async fn serve(stream: TcpStream, clients: Arc<Semaphore>) {
    let mut permit = None;
    let config = WebSocketConfig::default().max_message_size(Some(16384));
    // The callback error type is fixed by tungstenite.
    #[allow(clippy::result_large_err)]
    let callback = |request: &Request, response: Response| {
        permit = Some(
            authorize(request, clients)
                .map_err(|status| Response::builder().status(status).body(None).unwrap())?,
        );
        Ok(response)
    };
    let socket = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        accept_hdr_async_with_config(stream, callback, Some(config)),
    )
    .await;
    if let Ok(Ok(socket)) = socket {
        let _permit = permit;
        if let Err(error) = call::serve(socket).await {
            eprintln!("Media connection ended: {error}");
        }
    }
}

fn authorize(
    request: &Request,
    clients: Arc<Semaphore>,
) -> Result<OwnedSemaphorePermit, StatusCode> {
    let headers = request.headers();
    if request.uri().path() != "/media" {
        return Err(StatusCode::NOT_FOUND);
    }
    if headers.get(header::HOST).and_then(|h| h.to_str().ok()) != Some("127.0.0.1:38446")
        || !matches!(
            headers.get(header::ORIGIN).and_then(|h| h.to_str().ok()),
            Some(
                "https://discord.com"
                    | "https://canary.discord.com"
                    | "https://ptb.discord.com"
                    | "https://discordapp.com"
                    | "https://canary.discordapp.com"
                    | "https://ptb.discordapp.com"
            )
        )
    {
        return Err(StatusCode::FORBIDDEN);
    }
    clients
        .try_acquire_owned()
        .map_err(|_| StatusCode::CONFLICT)
}
