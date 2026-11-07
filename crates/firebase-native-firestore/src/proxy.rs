//! HTTP `CONNECT` proxy support for the gRPC transport.
//!
//! `tonic` does not consult proxy environment variables, while gRPC
//! core (and most HTTP tooling) honours them — and desktop users
//! behind corporate proxies need them. When the Firestore endpoint is
//! `https` and a proxy is configured via `grpc_proxy`, `https_proxy`
//! or `all_proxy` (lowercase or uppercase; `NO_PROXY` respected), the
//! channel is built over a connector that tunnels through the proxy
//! The connector returns the tunnelled TCP stream and `tonic`'s own
//! TLS configuration performs the handshake over it (native roots),
//! exactly as on the direct path. Without a proxy in the
//! environment, the plain `tonic` transport is used unchanged.

use std::io;
use std::sync::Arc;

use hyper_util::rt::TokioIo;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// A parsed forward-proxy configuration.
#[derive(Debug, Clone)]
pub(crate) struct ProxyConfig {
    /// Proxy `host:port` to dial.
    addr: String,
    /// Base64 credentials for `Proxy-Authorization`, when the proxy
    /// URL carried userinfo.
    auth: Option<String>,
}

impl ProxyConfig {
    /// The proxy to use for `target_host`, from the environment.
    pub(crate) fn from_env_for(target_host: &str) -> Option<ProxyConfig> {
        if no_proxy_matches(target_host) {
            return None;
        }
        let url = [
            "grpc_proxy",
            "GRPC_PROXY",
            "https_proxy",
            "HTTPS_PROXY",
            "all_proxy",
            "ALL_PROXY",
        ]
        .iter()
        .find_map(|name| std::env::var(name).ok())
        .filter(|value| !value.trim().is_empty())?;
        parse_proxy_url(&url)
    }
}

fn no_proxy_matches(target_host: &str) -> bool {
    let value = std::env::var("no_proxy")
        .or_else(|_| std::env::var("NO_PROXY"))
        .unwrap_or_default();
    let target = target_host.to_ascii_lowercase();
    value.split(',').map(str::trim).any(|entry| {
        if entry == "*" {
            return true;
        }
        let entry = entry.trim_start_matches('.').to_ascii_lowercase();
        !entry.is_empty() && (target == entry || target.ends_with(&format!(".{entry}")))
    })
}

/// Parses `http://[user[:password]@]host[:port]` proxy URLs. Only
/// plain HTTP proxies (the kind `CONNECT` runs over) are supported.
fn parse_proxy_url(url: &str) -> Option<ProxyConfig> {
    let rest = url.trim().strip_prefix("http://")?;
    let (userinfo, host_port) = match rest.rsplit_once('@') {
        Some((userinfo, host_port)) => (Some(userinfo), host_port),
        None => (None, rest),
    };
    let host_port = host_port.split('/').next()?.trim();
    if host_port.is_empty() {
        return None;
    }
    let addr = if host_port.contains(':') {
        host_port.to_string()
    } else {
        format!("{host_port}:3128")
    };
    let auth = userinfo.map(|info| base64_encode(info.as_bytes()));
    Some(ProxyConfig { addr, auth })
}

/// Tunnels a TCP connection to `target` through the proxy with
/// `CONNECT`. TLS is left to `tonic`'s transport configuration,
/// which wraps the returned stream.
pub(crate) async fn connect_via_proxy(
    proxy: Arc<ProxyConfig>,
    target_host: String,
    target_port: u16,
) -> io::Result<TokioIo<TcpStream>> {
    let mut stream = TcpStream::connect(&proxy.addr).await?;
    let authority = format!("{target_host}:{target_port}");
    let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
    if let Some(auth) = &proxy.auth {
        request.push_str(&format!("Proxy-Authorization: Basic {auth}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).await?;

    // Read the proxy response headers up to the blank line.
    let mut head = Vec::with_capacity(256);
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if stream.read_exact(&mut byte).await? == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "proxy closed the CONNECT tunnel",
            ));
        }
        head.push(byte[0]);
        if head.len() > 16 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "proxy CONNECT response headers too large",
            ));
        }
    }
    let status_line = String::from_utf8_lossy(&head);
    if !status_line.starts_with("HTTP/1.1 200") && !status_line.starts_with("HTTP/1.0 200") {
        return Err(io::Error::new(
            io::ErrorKind::ConnectionRefused,
            format!(
                "proxy CONNECT rejected: {}",
                status_line.lines().next().unwrap_or("")
            ),
        ));
    }

    Ok(TokioIo::new(stream))
}

/// Standard base64 encoding (for proxy credentials from the URL).
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0] as u32,
            *chunk.get(1).unwrap_or(&0) as u32,
            *chunk.get(2).unwrap_or(&0) as u32,
        ];
        let n = (b[0] << 16) | (b[1] << 8) | b[2];
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            ALPHABET[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            ALPHABET[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_proxy_urls() {
        let proxy = parse_proxy_url("http://proxy.example:3128").unwrap();
        assert_eq!(proxy.addr, "proxy.example:3128");
        assert_eq!(proxy.auth, None);

        let proxy = parse_proxy_url("http://user:pass@proxy.example").unwrap();
        assert_eq!(proxy.addr, "proxy.example:3128");
        assert_eq!(proxy.auth.as_deref(), Some("dXNlcjpwYXNz"));

        assert!(parse_proxy_url("https://proxy.example:3128").is_none());
        assert!(parse_proxy_url("http://").is_none());
    }

    #[test]
    fn base64_basics() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"a"), "YQ==");
        assert_eq!(base64_encode(b"ab"), "YWI=");
        assert_eq!(base64_encode(b"abc"), "YWJj");
        assert_eq!(base64_encode(b"user:pass"), "dXNlcjpwYXNz");
    }
}
