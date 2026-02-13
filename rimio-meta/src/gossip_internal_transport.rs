use crate::error::{MetaError, Result};
use bytes::Bytes;
use memberlist::agnostic::RuntimeLite;
use memberlist::proto::{Payload, ProtoReader, ProtoWriter};
use memberlist::tokio::{TokioRuntime, TokioSocketAddrResolver};
use memberlist::transport::{
    Connection, Packet, PacketProducer, PacketSubscriber, StreamProducer, StreamSubscriber,
    Transport, TransportError, packet_stream, promised_stream,
};
use reqwest::Client;
use smol_str::SmolStr;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Duration;
use tokio::sync::{Mutex as AsyncMutex, Notify};

const GOSSIP_PACKET_PATH: &str = "/internal/v1/gossip/packet";
const GOSSIP_STREAM_PATH: &str = "/internal/v1/gossip/stream";

static GLOBAL_INGRESS: OnceLock<StdMutex<Option<GossipInternalTransportIngress>>> = OnceLock::new();

fn global_ingress_cell() -> &'static StdMutex<Option<GossipInternalTransportIngress>> {
    GLOBAL_INGRESS.get_or_init(|| StdMutex::new(None))
}

pub fn install_global_ingress(ingress: GossipInternalTransportIngress) {
    let mut guard = global_ingress_cell()
        .lock()
        .expect("gossip internal transport ingress mutex poisoned");
    *guard = Some(ingress);
}

pub fn clear_global_ingress() {
    let mut guard = global_ingress_cell()
        .lock()
        .expect("gossip internal transport ingress mutex poisoned");
    *guard = None;
}

pub async fn ingest_global_packet(from: SocketAddr, payload: &[u8]) -> Result<bool> {
    let ingress = {
        let guard = global_ingress_cell()
            .lock()
            .expect("gossip internal transport ingress mutex poisoned");
        guard.clone()
    };

    let Some(ingress) = ingress else {
        return Ok(false);
    };

    ingress.ingest_packet(from, payload).await?;
    Ok(true)
}

pub async fn ingest_global_stream(from: SocketAddr, payload: &[u8]) -> Result<Option<Vec<u8>>> {
    let ingress = {
        let guard = global_ingress_cell()
            .lock()
            .expect("gossip internal transport ingress mutex poisoned");
        guard.clone()
    };

    let Some(ingress) = ingress else {
        return Ok(None);
    };

    let response = ingress.handle_stream_request(from, payload).await?;
    Ok(Some(response))
}

#[derive(Debug, Clone)]
pub struct GossipInternalTransportOptions {
    pub local_id: SmolStr,
    pub bind_addr: SocketAddr,
    pub advertise_addr: SocketAddr,
    pub request_timeout: Duration,
    pub(crate) packet_subscriber:
        PacketSubscriber<SocketAddr, <TokioRuntime as RuntimeLite>::Instant>,
    pub(crate) stream_subscriber: StreamSubscriber<SocketAddr, GossipInternalConnection>,
}

#[derive(Clone)]
pub struct GossipInternalTransportIngress {
    packet_producer: PacketProducer<SocketAddr, <TokioRuntime as RuntimeLite>::Instant>,
    stream_producer: StreamProducer<SocketAddr, GossipInternalConnection>,
    stream_timeout: Duration,
}

impl GossipInternalTransportIngress {
    pub(crate) fn new(
        packet_producer: PacketProducer<SocketAddr, <TokioRuntime as RuntimeLite>::Instant>,
        stream_producer: StreamProducer<SocketAddr, GossipInternalConnection>,
        stream_timeout: Duration,
    ) -> Self {
        Self {
            packet_producer,
            stream_producer,
            stream_timeout,
        }
    }

    pub async fn ingest_packet(&self, from: SocketAddr, payload: &[u8]) -> Result<()> {
        let packet = Packet::new(from, TokioRuntime::now(), Bytes::copy_from_slice(payload));
        self.packet_producer.send(packet).await.map_err(|error| {
            MetaError::Internal(format!(
                "failed to enqueue gossip packet for memberlist: {}",
                error
            ))
        })?;
        Ok(())
    }

    pub async fn handle_stream_request(&self, from: SocketAddr, payload: &[u8]) -> Result<Vec<u8>> {
        let shared = Arc::new(InboundShared::new(payload.to_vec()));
        let conn = GossipInternalConnection::inbound(Arc::clone(&shared));

        self.stream_producer
            .send(from, conn)
            .await
            .map_err(|error| {
                MetaError::Internal(format!(
                    "failed to enqueue gossip stream for memberlist: {}",
                    error
                ))
            })?;

        shared
            .wait_response(self.stream_timeout)
            .await
            .map_err(|error| {
                MetaError::Internal(format!("gossip stream response error: {}", error))
            })
    }
}

pub(crate) fn new_transport_channels() -> (
    PacketProducer<SocketAddr, <TokioRuntime as RuntimeLite>::Instant>,
    PacketSubscriber<SocketAddr, <TokioRuntime as RuntimeLite>::Instant>,
    StreamProducer<SocketAddr, GossipInternalConnection>,
    StreamSubscriber<SocketAddr, GossipInternalConnection>,
) {
    let (packet_producer, packet_subscriber) = packet_stream::<GossipInternalHttpTransport>();
    let (stream_producer, stream_subscriber) = promised_stream::<GossipInternalHttpTransport>();
    (
        packet_producer,
        packet_subscriber,
        stream_producer,
        stream_subscriber,
    )
}

#[derive(Debug, thiserror::Error)]
pub enum GossipInternalTransportError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("http error: {0}")]
    Http(String),
    #[error("remote error: {0}")]
    Remote(String),
    #[error("transport error: {0}")]
    Other(String),
}

impl TransportError for GossipInternalTransportError {
    fn is_remote_failure(&self) -> bool {
        matches!(self, Self::Http(_) | Self::Remote(_))
    }

    fn custom(err: std::borrow::Cow<'static, str>) -> Self {
        Self::Other(err.into_owned())
    }
}

impl From<reqwest::Error> for GossipInternalTransportError {
    fn from(value: reqwest::Error) -> Self {
        Self::Http(value.to_string())
    }
}

impl From<GossipInternalTransportError> for MetaError {
    fn from(value: GossipInternalTransportError) -> Self {
        MetaError::Internal(value.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct GossipInternalHttpTransport {
    local_id: SmolStr,
    bind_addr: SocketAddr,
    advertise_addr: SocketAddr,
    request_timeout: Duration,
    http_client: Client,
    packet_subscriber: PacketSubscriber<SocketAddr, <TokioRuntime as RuntimeLite>::Instant>,
    stream_subscriber: StreamSubscriber<SocketAddr, GossipInternalConnection>,
}

impl GossipInternalHttpTransport {
    fn build_url(addr: &SocketAddr, path: &str) -> String {
        format!("http://{}{}", addr, path)
    }
}

impl Transport for GossipInternalHttpTransport {
    type Error = GossipInternalTransportError;
    type Id = SmolStr;
    type Address = SocketAddr;
    type ResolvedAddress = SocketAddr;
    type Resolver = TokioSocketAddrResolver;
    type Connection = GossipInternalConnection;
    type Runtime = TokioRuntime;
    type Options = GossipInternalTransportOptions;

    async fn new(options: Self::Options) -> std::result::Result<Self, Self::Error> {
        Ok(Self {
            local_id: options.local_id,
            bind_addr: options.bind_addr,
            advertise_addr: options.advertise_addr,
            request_timeout: options.request_timeout,
            http_client: Client::builder().timeout(options.request_timeout).build()?,
            packet_subscriber: options.packet_subscriber,
            stream_subscriber: options.stream_subscriber,
        })
    }

    async fn resolve(
        &self,
        addr: &<Self::Resolver as memberlist::transport::AddressResolver>::Address,
    ) -> std::result::Result<Self::ResolvedAddress, Self::Error> {
        Ok(*addr)
    }

    fn local_id(&self) -> &Self::Id {
        &self.local_id
    }

    fn local_address(
        &self,
    ) -> &<Self::Resolver as memberlist::transport::AddressResolver>::Address {
        &self.bind_addr
    }

    fn advertise_address(&self) -> &Self::ResolvedAddress {
        &self.advertise_addr
    }

    fn max_packet_size(&self) -> usize {
        1024 * 1024
    }

    fn header_overhead(&self) -> usize {
        0
    }

    fn blocked_address(
        &self,
        _addr: &Self::ResolvedAddress,
    ) -> std::result::Result<(), Self::Error> {
        Ok(())
    }

    async fn send_to(
        &self,
        addr: &Self::ResolvedAddress,
        packet: Payload,
    ) -> std::result::Result<(usize, <Self::Runtime as RuntimeLite>::Instant), Self::Error> {
        let payload = packet.as_slice().to_vec();
        let size = payload.len();
        let url = Self::build_url(addr, GOSSIP_PACKET_PATH);

        let response = self
            .http_client
            .post(url)
            .query(&[("from", self.advertise_addr.to_string())])
            .body(payload)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(Self::Error::Remote(format!(
                "gossip packet endpoint returned {}",
                response.status()
            )));
        }

        Ok((size, TokioRuntime::now()))
    }

    async fn open(
        &self,
        addr: &Self::ResolvedAddress,
        _deadline: <Self::Runtime as RuntimeLite>::Instant,
    ) -> std::result::Result<Self::Connection, Self::Error> {
        Ok(GossipInternalConnection::outbound(
            Arc::new(OutboundShared::new()),
            self.http_client.clone(),
            Self::build_url(addr, GOSSIP_STREAM_PATH),
            self.advertise_addr,
            self.request_timeout,
        ))
    }

    fn packet(
        &self,
    ) -> PacketSubscriber<Self::ResolvedAddress, <Self::Runtime as RuntimeLite>::Instant> {
        self.packet_subscriber.clone()
    }

    fn stream(&self) -> StreamSubscriber<Self::ResolvedAddress, Self::Connection> {
        self.stream_subscriber.clone()
    }

    fn packet_reliable(&self) -> bool {
        true
    }

    fn packet_secure(&self) -> bool {
        false
    }

    fn stream_secure(&self) -> bool {
        false
    }

    async fn shutdown(&self) -> std::result::Result<(), Self::Error> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum GossipInternalConnection {
    Outbound(Arc<OutboundShared>, Client, String, SocketAddr, Duration),
    Inbound(Arc<InboundShared>),
}

impl GossipInternalConnection {
    fn outbound(
        shared: Arc<OutboundShared>,
        client: Client,
        url: String,
        from: SocketAddr,
        timeout: Duration,
    ) -> Self {
        Self::Outbound(shared, client, url, from, timeout)
    }

    fn inbound(shared: Arc<InboundShared>) -> Self {
        Self::Inbound(shared)
    }
}

impl Connection for GossipInternalConnection {
    type Reader = GossipInternalReader;
    type Writer = GossipInternalWriter;

    fn split(self) -> (Self::Reader, Self::Writer) {
        match self {
            Self::Outbound(shared, client, url, from, timeout) => (
                GossipInternalReader::Outbound {
                    shared: Arc::clone(&shared),
                    cursor: 0,
                    loaded: false,
                },
                GossipInternalWriter::Outbound {
                    shared,
                    client,
                    url,
                    from,
                    timeout,
                    sent: false,
                },
            ),
            Self::Inbound(shared) => (
                GossipInternalReader::Inbound {
                    data: shared.request.clone(),
                    cursor: 0,
                },
                GossipInternalWriter::Inbound {
                    shared,
                    buffer: Vec::new(),
                },
            ),
        }
    }

    fn read(
        &mut self,
        _buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
        async { Err(std::io::Error::other("use split() before read")) }
    }

    fn read_exact(
        &mut self,
        _buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async { Err(std::io::Error::other("use split() before read_exact")) }
    }

    fn peek(
        &mut self,
        _buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
        async { Err(std::io::Error::other("use split() before peek")) }
    }

    fn peek_exact(
        &mut self,
        _buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async { Err(std::io::Error::other("use split() before peek_exact")) }
    }

    fn consume_peek(&mut self) {}

    fn write_all(
        &mut self,
        _payload: &[u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async { Err(std::io::Error::other("use split() before write_all")) }
    }

    fn flush(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async { Err(std::io::Error::other("use split() before flush")) }
    }

    fn close(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async { Err(std::io::Error::other("use split() before close")) }
    }
}

#[derive(Debug, Clone)]
pub enum GossipInternalReader {
    Outbound {
        shared: Arc<OutboundShared>,
        cursor: usize,
        loaded: bool,
    },
    Inbound {
        data: Vec<u8>,
        cursor: usize,
    },
}

impl GossipInternalReader {
    async fn ensure_outbound_loaded(shared: &Arc<OutboundShared>) -> std::io::Result<Vec<u8>> {
        loop {
            let guard = shared.inner.lock().await;
            if let Some(error) = &guard.error {
                return Err(std::io::Error::other(error.clone()));
            }
            if let Some(response) = &guard.response {
                return Ok(response.clone());
            }
            drop(guard);
            shared.notify.notified().await;
        }
    }
}

impl ProtoReader for GossipInternalReader {
    fn read(
        &mut self,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
        async move {
            match self {
                Self::Inbound { data, cursor } => {
                    let remaining = data.len().saturating_sub(*cursor);
                    if remaining == 0 {
                        return Ok(0);
                    }
                    let take = remaining.min(buf.len());
                    buf[..take].copy_from_slice(&data[*cursor..*cursor + take]);
                    *cursor += take;
                    Ok(take)
                }
                Self::Outbound {
                    shared,
                    cursor,
                    loaded,
                } => {
                    if !*loaded {
                        let data = Self::ensure_outbound_loaded(shared).await?;
                        let mut guard = shared.inner.lock().await;
                        guard.cached = Some(data);
                        *loaded = true;
                    }

                    let guard = shared.inner.lock().await;
                    let data = guard.cached.as_ref().cloned().unwrap_or_default();
                    let remaining = data.len().saturating_sub(*cursor);
                    if remaining == 0 {
                        return Ok(0);
                    }
                    let take = remaining.min(buf.len());
                    buf[..take].copy_from_slice(&data[*cursor..*cursor + take]);
                    *cursor += take;
                    Ok(take)
                }
            }
        }
    }

    fn read_exact(
        &mut self,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async move {
            let mut read = 0usize;
            while read < buf.len() {
                let n = self.read(&mut buf[read..]).await?;
                if n == 0 {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "unexpected eof while reading exact",
                    ));
                }
                read += n;
            }
            Ok(())
        }
    }

    fn peek(
        &mut self,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<usize>> + Send {
        async move {
            match self {
                Self::Inbound { data, cursor } => {
                    let remaining = data.len().saturating_sub(*cursor);
                    if remaining == 0 {
                        return Ok(0);
                    }
                    let take = remaining.min(buf.len());
                    buf[..take].copy_from_slice(&data[*cursor..*cursor + take]);
                    Ok(take)
                }
                Self::Outbound {
                    shared,
                    cursor,
                    loaded,
                } => {
                    if !*loaded {
                        let data = Self::ensure_outbound_loaded(shared).await?;
                        let mut guard = shared.inner.lock().await;
                        guard.cached = Some(data);
                        *loaded = true;
                    }

                    let guard = shared.inner.lock().await;
                    let data = guard.cached.as_ref().cloned().unwrap_or_default();
                    let remaining = data.len().saturating_sub(*cursor);
                    if remaining == 0 {
                        return Ok(0);
                    }
                    let take = remaining.min(buf.len());
                    buf[..take].copy_from_slice(&data[*cursor..*cursor + take]);
                    Ok(take)
                }
            }
        }
    }

    fn peek_exact(
        &mut self,
        buf: &mut [u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async move {
            let n = self.peek(buf).await?;
            if n < buf.len() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unexpected eof while peeking exact",
                ));
            }
            Ok(())
        }
    }
}

#[derive(Debug, Clone)]
pub enum GossipInternalWriter {
    Outbound {
        shared: Arc<OutboundShared>,
        client: Client,
        url: String,
        from: SocketAddr,
        timeout: Duration,
        sent: bool,
    },
    Inbound {
        shared: Arc<InboundShared>,
        buffer: Vec<u8>,
    },
}

impl ProtoWriter for GossipInternalWriter {
    fn write_all(
        &mut self,
        payload: &[u8],
    ) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async move {
            match self {
                Self::Outbound { shared, .. } => {
                    let mut guard = shared.inner.lock().await;
                    guard.request.extend_from_slice(payload);
                    Ok(())
                }
                Self::Inbound { buffer, .. } => {
                    buffer.extend_from_slice(payload);
                    Ok(())
                }
            }
        }
    }

    fn flush(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async move {
            match self {
                Self::Outbound {
                    shared,
                    client,
                    url,
                    from,
                    timeout,
                    sent,
                } => {
                    if *sent {
                        return Ok(());
                    }
                    let request_payload = {
                        let mut guard = shared.inner.lock().await;
                        if guard.sent {
                            *sent = true;
                            return Ok(());
                        }
                        guard.sent = true;
                        guard.request.clone()
                    };

                    let response = client
                        .post(url.clone())
                        .query(&[("from", from.to_string())])
                        .timeout(*timeout)
                        .body(request_payload)
                        .send()
                        .await;

                    match response {
                        Ok(resp) => {
                            if !resp.status().is_success() {
                                let message =
                                    format!("gossip stream endpoint returned {}", resp.status());
                                let mut guard = shared.inner.lock().await;
                                guard.error = Some(message.clone());
                                shared.notify.notify_waiters();
                                return Err(std::io::Error::other(message));
                            }

                            let bytes = resp.bytes().await.map_err(std::io::Error::other)?;
                            let mut guard = shared.inner.lock().await;
                            guard.response = Some(bytes.to_vec());
                            shared.notify.notify_waiters();
                            *sent = true;
                            Ok(())
                        }
                        Err(error) => {
                            let message = error.to_string();
                            let mut guard = shared.inner.lock().await;
                            guard.error = Some(message.clone());
                            shared.notify.notify_waiters();
                            Err(std::io::Error::other(message))
                        }
                    }
                }
                Self::Inbound { shared, buffer } => {
                    shared.set_response(buffer.clone()).await;
                    Ok(())
                }
            }
        }
    }

    fn close(&mut self) -> impl std::future::Future<Output = std::io::Result<()>> + Send {
        async move { self.flush().await }
    }
}

#[derive(Debug)]
pub struct OutboundState {
    request: Vec<u8>,
    response: Option<Vec<u8>>,
    error: Option<String>,
    sent: bool,
    cached: Option<Vec<u8>>,
}

#[derive(Debug)]
pub struct OutboundShared {
    inner: AsyncMutex<OutboundState>,
    notify: Notify,
}

impl OutboundShared {
    fn new() -> Self {
        Self {
            inner: AsyncMutex::new(OutboundState {
                request: Vec::new(),
                response: None,
                error: None,
                sent: false,
                cached: None,
            }),
            notify: Notify::new(),
        }
    }
}

#[derive(Debug)]
pub struct InboundShared {
    request: Vec<u8>,
    response: AsyncMutex<Option<Vec<u8>>>,
    notify: Notify,
}

impl InboundShared {
    fn new(request: Vec<u8>) -> Self {
        Self {
            request,
            response: AsyncMutex::new(None),
            notify: Notify::new(),
        }
    }

    async fn set_response(&self, payload: Vec<u8>) {
        let mut guard = self.response.lock().await;
        if guard.is_none() {
            *guard = Some(payload);
            self.notify.notify_waiters();
        }
    }

    async fn wait_response(&self, timeout: Duration) -> std::io::Result<Vec<u8>> {
        let fut = async {
            loop {
                let guard = self.response.lock().await;
                if let Some(payload) = &*guard {
                    return Ok(payload.clone());
                }
                drop(guard);
                self.notify.notified().await;
            }
        };

        tokio::time::timeout(timeout, fut).await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::TimedOut, "gossip stream timeout")
        })?
    }
}
