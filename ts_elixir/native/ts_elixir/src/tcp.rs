use std::sync::Arc;

use rustler::{Encoder, ResourceArc};

use crate::{IpOrSelf, Result, TOKIO_RUNTIME, atoms, erl_result, ip_from_erl, ok_arc};

pub(crate) struct TcpListener {
    inner: Arc<tailscale::netstack::TcpListener>,
}

pub(crate) struct TcpStream {
    inner: Arc<tailscale::netstack::TcpStream>,
}

#[rustler::resource_impl]
impl rustler::Resource for TcpListener {}

#[rustler::resource_impl]
impl rustler::Resource for TcpStream {}

#[rustler::nif(schedule = "DirtyIo")]
fn tcp_listen(
    env: rustler::Env,
    dev: ResourceArc<crate::Device>,
    addr: rustler::Term,
    port: u16,
) -> impl Encoder {
    let dev = dev.inner.clone();
    let ip = IpOrSelf::new(addr);

    let sock = TOKIO_RUNTIME.block_on(async move {
        let addr = ip.ok_or("invalid ip addr")?.resolve(&dev).await?;
        let sock = dev.tcp_listen((addr, port).into()).await?;

        ok_arc(TcpListener {
            inner: Arc::new(sock),
        })
    });

    erl_result(env, sock)
}

#[rustler::nif]
fn tcp_listen_local_addr(env: rustler::Env, listener: ResourceArc<TcpListener>) -> impl Encoder {
    crate::sockaddr_to_erl(env, listener.inner.local_addr())
}

#[rustler::nif(schedule = "DirtyIo")]
fn tcp_connect(
    env: rustler::Env<'_>,
    dev: ResourceArc<crate::Device>,
    addr: rustler::Term,
    port: u16,
) -> impl Encoder {
    let addr = ip_from_erl(addr);
    let dev = dev.inner.clone();

    let sock = TOKIO_RUNTIME.block_on(async move {
        let addr = addr.ok_or("invalid ip addr")?;
        let sock = dev.tcp_connect((addr, port).into()).await?;

        ok_arc(TcpStream {
            inner: Arc::new(sock),
        })
    });

    erl_result(env, sock)
}

#[rustler::nif(schedule = "DirtyIo")]
fn tcp_accept(env: rustler::Env<'_>, sock: ResourceArc<TcpListener>) -> impl Encoder {
    let inner = sock.inner.clone();

    let sock = TOKIO_RUNTIME.block_on(async move {
        let stream = inner.accept().await?;

        ok_arc(TcpStream {
            inner: Arc::new(stream),
        })
    });

    erl_result(env, sock)
}

#[rustler::nif(schedule = "DirtyIo")]
fn tcp_send(env: rustler::Env, sock: ResourceArc<TcpStream>, msg: Vec<u8>) -> rustler::Term {
    let inner = sock.inner.clone();

    match TOKIO_RUNTIME.block_on(async move { inner.send(&msg).await }) {
        Ok(n) => (atoms::ok(), n).encode(env),
        Err(e) => (atoms::error(), e.to_string()).encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn tcp_recv(env: rustler::Env, sock: ResourceArc<TcpStream>) -> impl Encoder {
    let inner = sock.inner.clone();

    let buf = TOKIO_RUNTIME.block_on(async move {
        let buf = inner.recv_bytes().await?;
        Result::<_>::Ok(buf.to_vec())
    });

    erl_result(env, buf)
}

/// Receive up to `max_len` bytes from the stream, with an optional timeout.
///
/// `max_len`: maximum number of bytes to read. If 0, returns whatever is available.
/// `timeout_ms`: timeout in milliseconds, or 0 for no timeout (block indefinitely).
///
/// Blocks until at least one byte is available or the timeout expires.
/// Returns `{:error, "timeout"}` if the timeout expires before data arrives.
#[rustler::nif(schedule = "DirtyIo")]
fn tcp_recv_max(
    env: rustler::Env,
    sock: ResourceArc<TcpStream>,
    max_len: usize,
    timeout_ms: u64,
) -> impl Encoder {
    let inner = sock.inner.clone();

    let buf = TOKIO_RUNTIME.block_on(async move {
        let fut = async {
            if max_len == 0 {
                let buf = inner.recv_bytes().await?;
                Result::<_>::Ok(buf.to_vec())
            } else {
                let mut buf = vec![0u8; max_len];
                let n = inner.recv(&mut buf).await?;
                buf.truncate(n);
                Result::<_>::Ok(buf)
            }
        };

        if timeout_ms == 0 {
            fut.await
        } else {
            match tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), fut).await {
                Ok(result) => result,
                Err(_) => Err("timeout".into()),
            }
        }
    });

    erl_result(env, buf)
}

#[rustler::nif]
fn tcp_local_addr(env: rustler::Env, sock: ResourceArc<TcpStream>) -> impl Encoder {
    crate::sockaddr_to_erl(env, sock.inner.local_addr())
}

#[rustler::nif]
fn tcp_remote_addr(env: rustler::Env, sock: ResourceArc<TcpStream>) -> impl Encoder {
    crate::sockaddr_to_erl(env, sock.inner.remote_addr())
}
