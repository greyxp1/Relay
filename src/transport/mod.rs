//! Discord v8 UDP voice and bounded video transport. Call on a current-thread Tokio
//! runtime (or LocalSet). Native DAVE handles never cross a thread boundary.
//! No microphone, desktop, pipeline URL, or reconnect is implicit.
use crate::{
    capture::{self, Codec},
    dave,
};
use futures_util::{SinkExt, StreamExt};
use rand_core::{OsRng, RngCore};
use serde_json::{json, Value};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    net::{IpAddr, SocketAddr},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::{TcpStream, UdpSocket},
    sync::{
        mpsc::{self, error::TrySendError},
        watch,
    },
    time::{self, Instant},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};
use zeroize::Zeroizing;
mod av1;
mod camera;
mod h264;
mod h265;
mod mls;
mod pacer;
mod rate;
mod rtp;
use mls::Mls;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
const USERS: usize = 256;
const DEADLINE: Duration = Duration::from_secs(30);

// Deliberately neither Debug nor Serialize. Never include credentials or remote
// payloads/native MLS error strings in errors, callbacks, or logs.
#[derive(Clone)]
pub struct Credentials {
    /// Clone the same identity for all connections belonging to this call.
    pub identity: dave::Identity,
    pub server_id: String,
    pub channel_id: String,
    pub user_id: String,
    pub session_id: Zeroizing<String>,
    pub token: Zeroizing<String>,
    pub endpoint: String,
}

/// Each mode owns only the bounded queues it uses. Keep capacities at most 64.
pub enum Mode {
    /// `speaking` is the announced Speaking flag. Muted callers clear it and stop input.
    Voice {
        input: mpsc::Receiver<capture::Frame>,
        output: mpsc::Sender<ReceivedFrame>,
        speaking: watch::Receiver<bool>,
        video: mpsc::Sender<(u64, Option<camera::Picture>)>,
        keyframes: watch::Receiver<()>,
    },
    /// The caller must force a capture keyframe for each bounded request notification.
    /// `codecs` lists what capture can encode. `codec` starts as the codec capture
    /// produces and changes to Discord's choice, which capture must then switch to.
    /// Video frames in any other codec are dropped.
    GoLive {
        settings: capture::VideoSettings,
        codecs: Vec<Codec>,
        codec: watch::Sender<Codec>,
        video: mpsc::Receiver<capture::Frame>,
        audio: Option<mpsc::Receiver<capture::Frame>>,
        keyframes: mpsc::Sender<()>,
    },
}

impl Mode {
    fn video(&self) -> bool {
        !matches!(self, Self::Voice { .. })
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq)]
pub struct Status {
    /// True when the voice gateway and encrypted UDP transport are established.
    pub transport_ready: bool,
    pub video_bitrate: u32,
    /// Voice gateway heartbeat round trip in milliseconds.
    pub ping: u32,
}

/// Decrypted Opus and its RTP timing, ready for native playback.
/// The bounded output queue drops frames on overflow, never blocking heartbeats.
pub struct ReceivedFrame {
    pub marker: bool,
    pub ssrc: u32,
    pub sequence: u16,
    /// The sending user, mapped from its SSRC by Speaking events.
    pub user: u64,
    /// Wrapping RTP ticks at the codec's clock rate.
    pub timestamp: u32,
    pub data: Vec<u8>,
}

/// Cancel by dropping the future. Errors are sanitized.
/// The caller owns capture and drops it when this returns.
pub async fn run(
    credentials: Credentials,
    mode: Mode,
    mut report: impl FnMut(Status),
) -> Result<(), String> {
    Call::connect(credentials, mode, &mut report)
        .await?
        .run()
        .await
}

async fn send(ws: &mut Socket, message: Message) -> Result<(), String> {
    time::timeout(Duration::from_secs(5), ws.send(message))
        .await
        .map_err(|_| "Voice websocket write timed out")?
        .map_err(|_| "Voice websocket write failed".into())
}
async fn event(ws: &mut Socket, op: u8, data: Value) -> Result<(), String> {
    send(
        ws,
        Message::Text(json!({"op":op,"d":data}).to_string().into()),
    )
    .await
}
async fn binary(ws: &mut Socket, op: u8, bytes: Vec<u8>) -> Result<(), String> {
    if bytes.len() > 1024 * 1024 {
        return Err("DAVE output exceeded limit".into());
    }
    let mut payload = Vec::with_capacity(bytes.len() + 1);
    payload.push(op);
    payload.extend(bytes);
    send(ws, Message::Binary(payload.into())).await
}
fn number(v: &Value, key: &str) -> Result<u64, String> {
    v[key]
        .as_u64()
        .ok_or_else(|| "Malformed voice control message".into())
}
fn int<T: TryFrom<u64>>(v: &Value, key: &str) -> Result<T, String> {
    T::try_from(number(v, key)?).map_err(|_| "Voice control integer out of range".into())
}
fn codec_name(codec: Codec) -> &'static str {
    match codec {
        Codec::Opus => "opus",
        Codec::H264 => "H264",
        Codec::H265 => "H265",
        Codec::Av1 => "AV1",
    }
}
fn valid_id(text: &str) -> bool {
    text.len() <= 20 && text.bytes().all(|b| b.is_ascii_digit()) && text.parse::<u64>().is_ok()
}
fn id(v: &Value) -> Result<String, String> {
    v.as_str()
        .filter(|text| valid_id(text))
        .map(str::to_owned)
        .ok_or_else(|| "Malformed voice user ID".into())
}
fn version(v: &Value, key: &str, max: u16) -> Result<u16, String> {
    let value = int(v, key)?;
    if value == 0 || value > max {
        return Err("Unsupported DAVE version, plaintext forbidden".into());
    }
    Ok(value)
}
fn endpoint(value: &str) -> Result<String, String> {
    // Gateway supplies a host[:port], never an arbitrary application URL.
    let (host, port) = value
        .split_once(':')
        .map_or((value, None), |(h, p)| (h, Some(p)));
    if host.is_empty()
        || host.len() > 253
        || !host.ends_with(".discord.media")
        || !host
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'.')
        || port.is_some_and(|p| p.parse::<u16>().ok().filter(|p| *p != 0).is_none())
    {
        return Err("Invalid Discord voice endpoint".into());
    }
    Ok(format!("wss://{value}/?v=8"))
}

async fn next_frame(mode: &mut Mode, video_ready: bool) -> Option<capture::Frame> {
    match mode {
        Mode::Voice { input, .. } => input.recv().await,
        Mode::GoLive {
            video: input,
            audio: None,
            ..
        } if video_ready => input.recv().await,
        Mode::GoLive {
            video,
            audio: Some(audio),
            ..
        } => {
            tokio::select! { frame = video.recv(), if video_ready => frame, frame = audio.recv() => frame }
        }
        Mode::GoLive { .. } => std::future::pending().await,
    }
}

async fn sleep_until(deadline: Option<std::time::Instant>) {
    match deadline {
        Some(deadline) => time::sleep_until(Instant::from_std(deadline)).await,
        None => std::future::pending().await,
    }
}

async fn camera_keyframe(requests: &mut Option<watch::Receiver<()>>) {
    match requests {
        Some(keyframes) => {
            let _ = keyframes.changed().await;
        }
        _ => std::future::pending().await,
    }
}

async fn receive(socket: Option<&UdpSocket>, buffer: &mut [u8]) -> std::io::Result<usize> {
    match socket {
        Some(socket) => socket.recv(buffer).await,
        None => std::future::pending().await,
    }
}

// Audio and video have independent RTP sequence numbers, clocks and reports.
struct Sender {
    sequence: u16,
    origin: u32,
    timestamp: u32,
    last_pts: Option<u64>,
    last_media: Instant,
    packets: u64,
    octets: u64,
}
impl Sender {
    fn new() -> Self {
        let origin = OsRng.next_u32();
        Self {
            sequence: OsRng.next_u32() as u16,
            origin,
            timestamp: origin,
            last_pts: None,
            last_media: Instant::now(),
            packets: 0,
            octets: 0,
        }
    }

    /// Repairs pass no timestamp and leave the media clock mapping alone.
    fn sent(&mut self, timestamp: Option<u32>, octets: usize) {
        if let Some(timestamp) = timestamp {
            self.timestamp = timestamp;
            self.last_media = Instant::now();
        }
        self.sequence = self.sequence.wrapping_add(1);
        self.packets = self.packets.saturating_add(1);
        self.octets = self.octets.saturating_add(octets as u64);
    }
}

struct Call<'a> {
    mode: Mode,
    identity: dave::Identity,
    user_id: String,
    max_version: u16,
    status: Status,
    on_status: &'a mut dyn FnMut(Status),
    ws: Socket,
    // Signaling and MLS.
    users: BTreeSet<String>,
    ssrc_users: BTreeMap<u32, String>,
    mls: Option<Mls>,
    external: Option<Vec<u8>>,
    group: u64,
    dave_ready: bool,
    recoveries: u8,
    seq_ack: i64,
    heartbeat: Option<Duration>,
    next_heartbeat: Instant,
    ack: Option<(u64, Instant)>,
    negotiation_timeout: Duration,
    startup: Instant,
    // UDP transport.
    udp: Option<UdpSocket>,
    cipher: Option<rtp::Cipher>,
    discovered: bool,
    ssrc: u32,
    video_ssrc: Option<(u32, u32)>,
    // Go Live sending.
    video_codec: Codec,
    /// Discord wants no stream video for now.
    paused: bool,
    pacing: Option<(pacer::Pacer, rate::Rate)>,
    audio_sender: Sender,
    video_sender: Sender,
    rtx_sender: Sender,
    transport_sequence: u16,
    need_keyframe: bool,
    last_keyframe_request: Option<Instant>,
    media_epoch: Option<u64>,
    next_report: Instant,
    cameras: BTreeMap<u32, camera::Camera>,
    pictures: mpsc::Sender<camera::Picture>,
    incoming_pictures: mpsc::Receiver<camera::Picture>,
    next_camera_keyframe: Instant,
    camera_requests: Option<watch::Receiver<()>>,
}

impl<'a> Call<'a> {
    async fn connect(
        c: Credentials,
        mode: Mode,
        on_status: &'a mut dyn FnMut(Status),
    ) -> Result<Self, String> {
        let mut status = Status::default();
        let mut video_codec = Codec::H264;
        let pacing = match &mode {
            Mode::GoLive {
                settings,
                codecs,
                codec,
                ..
            } => {
                settings.validate()?;
                if codecs.is_empty() || codecs.contains(&Codec::Opus) {
                    return Err("Invalid stream video codecs".into());
                }
                video_codec = *codec.borrow();
                let now = std::time::Instant::now();
                let rate = rate::Rate::new(settings.bitrate, now);
                status.video_bitrate = rate.bitrate();
                Some((pacer::Pacer::new(rate.bitrate(), now), rate))
            }
            _ => None,
        };
        let channel = c
            .channel_id
            .parse::<u64>()
            .map_err(|_| "Invalid channel ID")?;
        if !valid_id(&c.user_id) || !valid_id(&c.server_id) {
            return Err("Malformed voice user ID".into());
        }
        if c.session_id.is_empty()
            || c.token.is_empty()
            || c.session_id.len() > 4096
            || c.token.len() > 4096
        {
            return Err("Invalid voice credentials".into());
        }
        let max_version = dave::DaveLibrary::load()
            .map_err(|_| "DAVE library unavailable")?
            .max_supported_protocol_version();
        if max_version == 0 {
            return Err("Encrypted DAVE unavailable".into());
        }
        let config = WebSocketConfig::default()
            .max_message_size(Some(1024 * 1024))
            .max_frame_size(Some(1024 * 1024));
        let (mut ws, _) = time::timeout(
            Duration::from_secs(15),
            connect_async_with_config(endpoint(&c.endpoint)?, Some(config), false),
        )
        .await
        .map_err(|_| "Voice websocket connection timed out")?
        .map_err(|_| "Voice websocket connection failed")?;
        let video = mode.video();
        let mut identify = json!({"server_id":c.server_id,"channel_id":c.channel_id,"user_id":c.user_id,"session_id":c.session_id.as_str(),"token":c.token.as_str(),"max_dave_protocol_version":max_version});
        identify["video"] = json!(true);
        if video {
            identify["streams"] = json!([{"type":"screen","rid":"100","quality":100}]);
        }
        event(&mut ws, 0, identify).await?;
        let negotiation_timeout = if video {
            Duration::from_secs(90)
        } else {
            DEADLINE
        };
        let now = Instant::now();
        let (pictures, incoming_pictures) = mpsc::channel(8);
        let camera_requests = match &mode {
            Mode::Voice { keyframes, .. } => Some(keyframes.clone()),
            _ => None,
        };
        Ok(Self {
            mode,
            identity: c.identity,
            users: BTreeSet::from([c.user_id.clone()]),
            user_id: c.user_id,
            max_version,
            status,
            on_status,
            ws,
            ssrc_users: BTreeMap::new(),
            mls: None,
            external: None,
            group: channel,
            dave_ready: false,
            recoveries: 0,
            seq_ack: -1,
            heartbeat: None,
            next_heartbeat: now,
            ack: None,
            negotiation_timeout,
            startup: now + negotiation_timeout,
            udp: None,
            cipher: None,
            discovered: false,
            ssrc: 0,
            video_ssrc: None,
            video_codec,
            paused: false,
            pacing,
            audio_sender: Sender::new(),
            video_sender: Sender::new(),
            rtx_sender: Sender::new(),
            transport_sequence: 1,
            need_keyframe: video,
            last_keyframe_request: None,
            media_epoch: None,
            next_report: now + Duration::from_secs(5),
            cameras: BTreeMap::new(),
            pictures,
            incoming_pictures,
            next_camera_keyframe: now,
            camera_requests,
        })
    }

    async fn run(mut self) -> Result<(), String> {
        let mut tick = time::interval(Duration::from_millis(100));
        tick.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
        let mut buffer = [0; 4096];
        loop {
            self.pacer_upkeep()?;
            let deadline = self
                .pacing
                .as_ref()
                .and_then(|(pacer, _)| pacer.deadline(std::time::Instant::now()));
            // Propagate pacer pressure to raw-frame admission instead of dropping encoded references.
            let video_ready = self
                .pacing
                .as_ref()
                .is_none_or(|(pacer, _)| pacer.can_enqueue());
            tokio::select! {
                _ = sleep_until(deadline) => self.send_video().await?,
                _ = tick.tick() => self.tick().await?,
                Some(picture) = self.incoming_pictures.recv() => self.camera_picture(picture),
                _ = camera_keyframe(&mut self.camera_requests) => {
                    for camera in self.cameras.values_mut() { camera.seen = Instant::now() - Duration::from_secs(2); }
                    self.next_camera_keyframe = Instant::now();
                }
                frame = next_frame(&mut self.mode, video_ready) => {
                    self.send_frame(frame.ok_or("Media input ended")?).await?;
                }
                size = receive(self.udp.as_ref(), &mut buffer) => {
                    let size = size.map_err(|_| "Voice UDP receive failed")?;
                    self.on_udp(&mut buffer[..size]).await?;
                }
                message = self.ws.next() => {
                    let message = message
                        .ok_or("Voice websocket ended")?
                        .map_err(|_| "Voice websocket receive failed")?;
                    self.on_message(message).await?;
                    self.update_ready().await?;
                }
            }
        }
    }

    fn report(&mut self) {
        (self.on_status)(self.status);
    }

    async fn stop_camera(&mut self, user: &str) -> Result<(), String> {
        let previous = self.cameras.len();
        self.cameras.retain(|_, camera| camera.user != user);
        if previous != self.cameras.len() {
            if let Mode::Voice { video, .. } = &self.mode {
                video
                    .send((user.parse().map_err(|_| "Invalid camera user")?, None))
                    .await
                    .map_err(|_| "Camera receiver closed")?;
            }
        }
        Ok(())
    }

    async fn camera_streams(&mut self, data: &Value) -> Result<(), String> {
        let user = id(&data["user_id"])?;
        if user == self.user_id || !self.users.contains(&user) {
            return Ok(());
        }
        let streams = data["streams"].as_array().filter(|s| s.len() <= 8);
        let selected = streams.and_then(|streams| {
            streams
                .iter()
                .filter(|s| s["active"].as_bool() == Some(true))
                .max_by_key(|s| s["quality"].as_u64().unwrap_or(0))
        });
        let selected = selected.or_else(|| {
            (streams.is_none() && data["video_ssrc"].as_u64().unwrap_or(0) != 0).then_some(data)
        });
        let Some(stream) = selected else {
            self.stop_camera(&user).await?;
            return Ok(());
        };
        let ssrc = stream["ssrc"]
            .as_u64()
            .or_else(|| stream["video_ssrc"].as_u64())
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(0);
        if ssrc == 0 || self.cameras.contains_key(&ssrc) {
            return Ok(());
        }
        let rtx = stream["rtx_ssrc"]
            .as_u64()
            .and_then(|n| u32::try_from(n).ok())
            .unwrap_or(0);
        self.stop_camera(&user).await?;
        if self.cameras.len() >= 16 {
            return Ok(());
        }
        let pictures = self.pictures.clone();
        let camera =
            tokio::task::spawn_blocking(move || camera::Camera::new(user, ssrc, rtx, pictures))
                .await
                .map_err(|_| "Camera receiver setup failed")??;
        self.cameras.insert(ssrc, camera);
        let wants: serde_json::Map<String, Value> = self
            .cameras
            .keys()
            .map(|s| (s.to_string(), json!(100)))
            .collect();
        event(&mut self.ws, 15, Value::Object(wants)).await?;
        self.next_camera_keyframe = Instant::now();
        Ok(())
    }

    fn camera_picture(&mut self, mut picture: camera::Picture) {
        let Some(camera) = self.cameras.get_mut(&picture.ssrc) else {
            return;
        };
        let Some(decryptor) = self
            .mls
            .as_mut()
            .and_then(|m| m.decryptors.get_mut(&camera.user))
        else {
            return;
        };
        let Ok(data) = decryptor.decrypt(Codec::H264, &picture.data) else {
            picture.lost();
            return;
        };
        picture.data = data;
        let Ok(user) = camera.user.parse() else {
            return;
        };
        if let Mode::Voice { video, .. } = &self.mode {
            match video.try_send((user, Some(picture))) {
                Ok(()) => camera.seen = Instant::now(),
                Err(error) => error.into_inner().1.unwrap().lost(),
            }
        }
    }

    fn media_ready(&self) -> bool {
        self.status.transport_ready && self.mls.as_ref().is_some_and(|m| m.ready)
    }

    async fn send_sealed(&mut self, header: &[u8], data: &[&[u8]]) -> Result<(), String> {
        let packet = self
            .cipher
            .as_mut()
            .ok_or("Transport key missing")?
            .seal(header, data)?;
        self.udp
            .as_ref()
            .ok_or("UDP missing")?
            .send(&packet)
            .await
            .map_err(|_| "Voice UDP send failed")?;
        Ok(())
    }

    fn request_keyframe(&mut self) -> Result<(), String> {
        self.need_keyframe = true;
        self.last_keyframe_request = Some(Instant::now());
        if let Mode::GoLive { keyframes, .. } = &self.mode {
            if let Err(TrySendError::Closed(())) = keyframes.try_send(()) {
                return Err("Keyframe request channel closed".into());
            }
        }
        Ok(())
    }

    /// Adopts Discord's send codec and asks capture to switch when it differs.
    fn select_codec(&mut self, name: Option<&str>) -> Result<(), String> {
        let Mode::GoLive { codecs, codec, .. } = &self.mode else {
            return Ok(());
        };
        let selected = codecs
            .iter()
            .copied()
            .find(|codec| Some(codec_name(*codec)) == name)
            .ok_or("Discord selected an unoffered video codec")?;
        codec.send_if_modified(|current| std::mem::replace(current, selected) != selected);
        if selected != self.video_codec {
            self.video_codec = selected;
            self.restart_video()?;
        }
        Ok(())
    }

    /// Drops queued and repairable video, then waits for a fresh keyframe.
    fn restart_video(&mut self) -> Result<(), String> {
        if let Some((pacer, _)) = &mut self.pacing {
            pacer.clear(std::time::Instant::now());
        }
        self.request_keyframe()
    }

    fn pacer_upkeep(&mut self) -> Result<(), String> {
        let now = std::time::Instant::now();
        let ready = self.media_ready();
        let epoch = self.mls.as_ref().map(|m| m.generation);
        let restart = epoch != self.media_epoch || !ready;
        let Some((pacer, rate)) = &mut self.pacing else {
            return Ok(());
        };
        if restart {
            pacer.clear(now);
            rate.clear();
            self.media_epoch = epoch;
            self.need_keyframe = true;
        }
        self.need_keyframe |= pacer.expire(now);
        if !restart && !self.paused && pacer.can_probe() {
            if let Some(bitrate) = rate.probe(now) {
                pacer.probe(bitrate, now);
            }
        }
        if (restart && ready)
            || (ready
                && !self.paused
                && self.need_keyframe
                && self
                    .last_keyframe_request
                    .is_none_or(|last| last.elapsed() >= Duration::from_secs(1)))
        {
            self.request_keyframe()?;
        }
        Ok(())
    }

    async fn send_video(&mut self) -> Result<(), String> {
        // Drain earned tokens per wake, without imposing a timer wait on every packet.
        // The token bucket still limits bursts, and the batch bounds time away from audio.
        for _ in 0..64 {
            let (pacer, _) = self.pacing.as_mut().ok_or("Video pacer missing")?;
            let Some(packet) = pacer.pop(std::time::Instant::now()) else {
                break;
            };
            let (video, rtx) = self.video_ssrc.ok_or("Video SSRC missing")?;
            let repair = packet.original_sequence.is_some();
            let (sender, ssrc) = if repair {
                (&mut self.rtx_sender, rtx)
            } else {
                (&mut self.video_sender, video)
            };
            let sequence = sender.sequence;
            sender.sent(
                (!repair).then_some(packet.timestamp),
                packet.payload.len() + if repair { 2 } else { 0 },
            );
            let (header, size) = rtp::video_header(
                sequence,
                ssrc,
                self.video_codec,
                self.transport_sequence,
                &packet,
            );
            let transport_sequence = self.transport_sequence;
            self.transport_sequence = self.transport_sequence.wrapping_add(1);
            self.send_sealed(&header[..16], &[&header[16..size], &packet.payload])
                .await?;
            let now = std::time::Instant::now();
            let (pacer, rate) = self.pacing.as_mut().ok_or("Video pacer missing")?;
            // Sealing adds a 16 byte tag and 4 byte nonce.
            rate.sent(
                transport_sequence,
                size + packet.payload.len() + 20,
                packet.probe,
                now,
            );
            pacer.sent(sequence, packet, now);
        }
        Ok(())
    }

    async fn tick(&mut self) -> Result<(), String> {
        let now = Instant::now();
        if now >= self.next_camera_keyframe && !self.cameras.is_empty() && self.media_ready() {
            self.next_camera_keyframe = now + Duration::from_secs(2);
            for ssrc in self
                .cameras
                .iter()
                .filter(|(_, c)| {
                    c.recovery.load(std::sync::atomic::Ordering::Relaxed) & 1 != 0
                        || now.duration_since(c.seen) >= Duration::from_secs(1)
                })
                .map(|(ssrc, _)| *ssrc)
                .collect::<Vec<_>>()
            {
                let mut pli = vec![0x81, 206, 0, 2];
                pli.extend_from_slice(&self.ssrc.to_be_bytes());
                pli.extend_from_slice(&ssrc.to_be_bytes());
                self.send_sealed(&pli[..8], &[&pli[8..]]).await?;
            }
        }
        if self.users.len() == 1
            && self
                .mls
                .as_ref()
                .is_some_and(|m| !m.ready && m.pending.is_none())
        {
            self.startup = now + self.negotiation_timeout;
        }
        if (!self.media_ready() && now >= self.startup)
            || self
                .mls
                .as_ref()
                .and_then(|m| m.pending)
                .is_some_and(|p| now.duration_since(p.1) > DEADLINE)
        {
            return Err("Encrypted voice negotiation timed out".into());
        }
        if let Some(interval) = self.heartbeat {
            if self.ack.is_some_and(|(_, sent)| {
                now.duration_since(sent) >= interval + Duration::from_secs(5)
            }) {
                return Err("Voice heartbeat ACK timed out".into());
            }
            if now >= self.next_heartbeat && self.ack.is_none() {
                let nonce = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|_| "Invalid system clock")?
                    .as_millis() as u64;
                event(&mut self.ws, 3, json!({"t":nonce,"seq_ack":self.seq_ack})).await?;
                self.ack = Some((nonce, now));
                self.next_heartbeat = now + interval;
            }
        }
        if now >= self.next_report {
            let (video, rtx) = self.video_ssrc.unwrap_or((self.ssrc, self.ssrc));
            let reports = [
                (
                    &self.audio_sender,
                    &self.audio_sender,
                    self.ssrc,
                    Codec::Opus,
                ),
                (
                    &self.video_sender,
                    &self.video_sender,
                    video,
                    self.video_codec,
                ),
                (&self.rtx_sender, &self.video_sender, rtx, self.video_codec),
            ]
            .into_iter()
            .filter(|(sender, ..)| sender.packets != 0)
            .map(|(sender, mapping, ssrc, codec)| {
                let elapsed = now.duration_since(mapping.last_media).as_micros()
                    * u128::from(codec.clock())
                    / 1_000_000;
                rtp::sender_report(
                    ssrc,
                    mapping.timestamp.wrapping_add(elapsed as u32),
                    sender.packets,
                    sender.octets,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
            for report in reports {
                self.send_sealed(&report[..8], &[&report[8..]]).await?;
            }
            self.next_report = now + Duration::from_secs(5);
        }
        if self.media_ready() {
            self.announce().await?;
        }
        self.report();
        Ok(())
    }

    /// Sends a changed Speaking flag and returns whether voice may be sent.
    async fn announce(&mut self) -> Result<bool, String> {
        let Mode::Voice { speaking, .. } = &mut self.mode else {
            return Ok(true);
        };
        // A mute during the write is announced too, before any send is allowed.
        while speaking.has_changed().unwrap_or(false) {
            let on = *speaking.borrow_and_update();
            event(
                &mut self.ws,
                5,
                json!({"speaking":u8::from(on),"delay":0,"ssrc":self.ssrc}),
            )
            .await?;
        }
        Ok(*speaking.borrow())
    }

    async fn send_frame(&mut self, frame: capture::Frame) -> Result<(), String> {
        let video = frame.codec != Codec::Opus;
        if video && matches!(self.mode, Mode::Voice { .. }) {
            return Err("Encoded frame does not match transport mode".into());
        }
        let limit = if video { 1024 * 1024 } else { 4096 };
        if frame.data.is_empty() || frame.data.len() > limit {
            return Err("Invalid encoded media frame size".into());
        }
        if !self.media_ready() {
            return Ok(());
        }
        // Paused, or capture is still switching to Discord's codec.
        if video && (self.paused || frame.codec != self.video_codec) {
            self.need_keyframe = true;
            return Ok(());
        }
        // Audio queued before a mute is dropped, and an unmute is announced before it sends.
        if !video && !self.announce().await? {
            return Ok(());
        }
        if video && self.need_keyframe {
            if !frame.keyframe {
                return Ok(());
            }
            self.need_keyframe = false;
        }
        let sender = if video {
            &mut self.video_sender
        } else {
            &mut self.audio_sender
        };
        let pts = frame.pts.nseconds();
        if let Some(previous) = sender.last_pts.filter(|previous| pts <= *previous) {
            return Err(format!(
                "Nonmonotonic media timestamp ({:?}: {previous} -> {pts} ns)",
                frame.codec
            ));
        }
        sender.last_pts = Some(pts);
        let timestamp = sender.origin.wrapping_add(
            (u128::from(pts) * u128::from(frame.codec.clock()) / 1_000_000_000) as u32,
        );
        let ssrc = if video {
            self.video_ssrc.ok_or("Video SSRC missing")?.0
        } else {
            self.ssrc
        };
        // WebRTC rewrites SPS restrictions before DAVE decryption. Make
        // that edit before authentication so receivers leave the SPS intact.
        let plain = if frame.codec == Codec::H264 {
            h264::normalize(&frame.data)?
        } else {
            Cow::Borrowed(frame.data.as_slice())
        };
        // Encrypt the whole frame before adding any RTP descriptors.
        let data = self
            .mls
            .as_mut()
            .ok_or("DAVE missing")?
            .encryptor
            .encrypt(ssrc, frame.codec, &plain)
            .map_err(|_| "DAVE frame encryption failed")?;
        if video {
            let limit = rtp::MTU - 54;
            let payloads = match frame.codec {
                Codec::H265 => h265::payloads(&data, limit)?,
                Codec::Av1 => av1::payloads(&data, frame.keyframe, limit)?,
                _ => h264::payloads(&data, limit)?,
            };
            let (pacer, _) = self.pacing.as_mut().ok_or("Video pacer missing")?;
            if !pacer.enqueue(
                timestamp,
                frame.keyframe,
                payloads,
                std::time::Instant::now(),
            ) {
                self.need_keyframe = true;
            }
            return Ok(());
        }
        if data.len() > rtp::MTU - 32 {
            return Err("Opus frame exceeds transport MTU".into());
        }
        // Audio never waits behind the video pacer or its repair budget.
        let sender = &self.audio_sender;
        let header = rtp::header(sender.sequence, timestamp, ssrc, sender.packets == 0);
        self.send_sealed(&header, &[&data]).await?;
        self.audio_sender.sent(Some(timestamp), data.len());
        Ok(())
    }

    async fn on_udp(&mut self, packet: &mut [u8]) -> Result<(), String> {
        if !self.discovered {
            return self.on_discovery(packet).await;
        }
        let Some(cipher) = &self.cipher else {
            return Ok(());
        };
        if packet.len() >= 8 && packet[0] >> 6 == 2 && (192..=223).contains(&packet[1]) {
            if let Some(rtcp) = cipher.open(packet, 8) {
                self.on_rtcp(rtcp)?;
            }
            return Ok(());
        }
        let Some(clear) = rtp::clear_len(packet) else {
            return Ok(());
        };
        let Some(packet) = cipher.open(packet, clear) else {
            return Ok(());
        };
        let (header, plain) = packet.split_at(clear);
        if matches!(packet[1] & 127, rtp::H264_PT | rtp::H264_RTX_PT) {
            if let Some(media) = rtp::media(header, plain) {
                let repair = packet[1] & 127 == rtp::H264_RTX_PT;
                if let Some(camera) = self.cameras.iter().find_map(|(ssrc, camera)| {
                    ((*ssrc == media.ssrc && !repair) || (camera.rtx == media.ssrc && repair))
                        .then_some(camera)
                }) {
                    camera.push(media, repair);
                }
            }
            return Ok(());
        }
        match self.mode {
            Mode::GoLive { .. } => Ok(()),
            Mode::Voice { ref output, .. } => {
                let Some(packet) = rtp::opus(header, plain) else {
                    return Ok(());
                };
                let Some(user) = self.ssrc_users.get(&packet.ssrc) else {
                    return Ok(());
                };
                let Some(decryptor) = self.mls.as_mut().and_then(|m| m.decryptors.get_mut(user))
                else {
                    return Ok(());
                };
                let Ok(data) = decryptor.decrypt(Codec::Opus, packet.payload) else {
                    return Ok(());
                };
                if let Err(TrySendError::Closed(_)) = output.try_send(ReceivedFrame {
                    marker: packet.marker,
                    ssrc: packet.ssrc,
                    sequence: packet.sequence,
                    user: user.parse().unwrap_or_default(),
                    timestamp: packet.timestamp,
                    data,
                }) {
                    return Err("Playback queue closed".into());
                }
                Ok(())
            }
        }
    }

    async fn on_discovery(&mut self, packet: &[u8]) -> Result<(), String> {
        if packet.len() != 74
            || packet[..4] != [0, 2, 0, 70]
            || packet[4..8] != self.ssrc.to_be_bytes()
        {
            return Ok(());
        }
        let address = &packet[8..72];
        let end = address
            .iter()
            .position(|b| *b == 0)
            .ok_or("Invalid UDP discovery response")?;
        let address = std::str::from_utf8(&address[..end])
            .ok()
            .and_then(|address| address.parse::<IpAddr>().ok())
            .ok_or("Invalid UDP discovery address")?;
        let port = u16::from_be_bytes([packet[72], packet[73]]);
        if port == 0 {
            return Err("Invalid UDP discovery port".into());
        }
        let mut codecs =
            vec![json!({"name":"opus","type":"audio","priority":1000,"payload_type":120})];
        if self.mode.video() {
            let encode: &[Codec] = match &self.mode {
                Mode::GoLive { codecs, .. } => codecs,
                _ => &[],
            };
            // Discord prefers lower priorities.
            for (index, codec) in [Codec::Av1, Codec::H265, Codec::H264]
                .into_iter()
                .filter(|codec| *codec == Codec::H264 || encode.contains(codec))
                .enumerate()
            {
                let (media, rtx) = rtp::payload_types(codec);
                codecs.push(json!({"name":codec_name(codec),"type":"video","priority":(index + 1) * 1000,"payload_type":media,"rtx_payload_type":rtx,"encode":encode.contains(&codec),"decode":codec == Codec::H264}));
            }
        } else {
            codecs.push(json!({"name":"H264","type":"video","priority":1000,"payload_type":101,"rtx_payload_type":102,"encode":false,"decode":true}));
        }
        event(
            &mut self.ws,
            1,
            json!({"protocol":"udp","data":{"address":address.to_string(),"port":port,"mode":rtp::MODE},"codecs":codecs}),
        )
        .await?;
        self.discovered = true;
        Ok(())
    }

    fn on_rtcp(&mut self, rtcp: &[u8]) -> Result<(), String> {
        let Some((video, _)) = self.video_ssrc else {
            return Ok(());
        };
        let Some(feedback) = rtp::feedback(rtcp, video) else {
            return Ok(());
        };
        if feedback.keyframe
            && !self.need_keyframe
            && !self
                .pacing
                .as_ref()
                .is_some_and(|(pacer, _)| pacer.has_keyframe(std::time::Instant::now()))
        {
            // Repeated PLIs must not discard the keyframe already in flight or its repairs.
            self.request_keyframe()?;
        }
        let now = std::time::Instant::now();
        let (pacer, rate) = self.pacing.as_mut().ok_or("Video pacer missing")?;
        pacer.request(&feedback.nacks, now);
        let mut changed = feedback
            .receiver_bitrate
            .and_then(|bitrate| rate.receiver(bitrate, now));
        for report in &feedback.transport {
            changed = rate.feedback(report, now).or(changed);
        }
        self.set_bitrate(changed);
        Ok(())
    }

    /// Pauses at quality 0, otherwise follows the requested quality.
    fn on_sink_wants(&mut self, d: &Value) -> Result<(), String> {
        let (Mode::GoLive { settings, .. }, Some((video, _))) = (&self.mode, self.video_ssrc)
        else {
            return Ok(());
        };
        let settings = *settings;
        let key = video.to_string();
        let Some(quality) = d[&key].as_u64().or_else(|| d["any"].as_u64()) else {
            return Ok(());
        };
        let paused = quality == 0;
        if paused != self.paused {
            self.paused = paused;
            if paused {
                if let Some((pacer, _)) = &mut self.pacing {
                    pacer.clear(std::time::Instant::now());
                }
            } else {
                self.request_keyframe()?;
            }
        }
        // Hiding the player is not evidence that the connection lost bandwidth.
        if paused {
            return Ok(());
        }
        // Rendered pixel counts guide layer selection, not bitrate for our fixed-size layer.
        let share = quality.min(100) as f64 / 100.0;
        let cap = f64::from(settings.bitrate) * share;
        let changed = self
            .pacing
            .as_mut()
            .and_then(|(_, rate)| rate.set_cap(cap as u32, std::time::Instant::now()));
        self.set_bitrate(changed);
        Ok(())
    }

    /// Applies a changed send rate to the pacer and, through status, the encoder.
    fn set_bitrate(&mut self, bitrate: Option<u32>) {
        let (Some(bitrate), Some((pacer, _))) = (bitrate, &mut self.pacing) else {
            return;
        };
        pacer.set_bitrate(bitrate, std::time::Instant::now());
        self.status.video_bitrate = bitrate;
        self.report();
    }

    async fn on_message(&mut self, message: Message) -> Result<(), String> {
        match message {
            Message::Text(text) => self.on_text(&text).await,
            Message::Binary(bytes) => self.on_binary(&bytes).await,
            Message::Ping(bytes) => send(&mut self.ws, Message::Pong(bytes)).await,
            Message::Close(_) => Err("Voice websocket closed".into()),
            _ => Ok(()),
        }
    }

    async fn on_text(&mut self, text: &str) -> Result<(), String> {
        let payload: Value = serde_json::from_str(text).map_err(|_| "Malformed voice JSON")?;
        if let Some(seq) = payload["seq"].as_u64() {
            self.seq_ack = i64::try_from(seq).map_err(|_| "Invalid voice sequence")?;
        }
        let d = &payload["d"];
        match number(&payload, "op")? {
            8 => {
                if self.heartbeat.is_some() {
                    return Err("Duplicate voice Hello".into());
                }
                let interval = d["heartbeat_interval"]
                    .as_f64()
                    .filter(|v| v.is_finite() && *v >= 100.0 && *v <= 120_000.0)
                    .ok_or("Invalid voice heartbeat interval")?;
                self.heartbeat = Some(Duration::from_secs_f64(interval / 1000.0));
                self.next_heartbeat = Instant::now();
            }
            6 => {
                if let Some((_, sent)) = self
                    .ack
                    .filter(|(nonce, _)| d["t"].as_u64() == Some(*nonce))
                {
                    self.ack = None;
                    self.status.ping = sent.elapsed().as_millis() as u32;
                    self.report();
                }
            }
            14 => {
                if self.mode.video() {
                    if d["video_codec"].is_string() {
                        self.select_codec(d["video_codec"].as_str())?;
                    }
                    self.request_keyframe()?;
                }
            }
            2 => self.on_ready(d).await?,
            4 => self.on_session(d).await?,
            11 => {
                let added = d["user_ids"].as_array().ok_or("Invalid recognized users")?;
                if added.len() > USERS {
                    return Err("Recognized user limit exceeded".into());
                }
                for value in added {
                    self.users.insert(id(value)?);
                }
                if self.users.len() > USERS {
                    return Err("Recognized user limit exceeded".into());
                }
            }
            13 => {
                let user = id(&d["user_id"])?;
                if user == self.user_id {
                    return Err("Local voice user disconnected".into());
                }
                self.users.remove(&user);
                self.ssrc_users.retain(|_, u| *u != user);
                self.stop_camera(&user).await?;
            }
            12 if matches!(self.mode, Mode::Voice { .. }) => self.camera_streams(d).await?,
            15 => self.on_sink_wants(d)?,
            5 => {
                let user = id(&d["user_id"])?;
                if !self.users.contains(&user) {
                    return Err("Unrecognized speaking user".into());
                }
                let remote = int(d, "ssrc")?;
                if remote == self.ssrc {
                    return Err("Conflicting voice SSRC".into());
                }
                self.ssrc_users.retain(|_, u| *u != user);
                if self.ssrc_users.get(&remote).is_some_and(|u| *u != user) {
                    return Err("Conflicting voice SSRC".into());
                }
                self.ssrc_users.insert(remote, user);
            }
            21 => {
                let mls = self.mls.as_ref().ok_or("DAVE transition before session")?;
                if version(d, "protocol_version", self.max_version)?
                    != mls.session.protocol_version()
                {
                    return Err("Unprepared DAVE version change".into());
                }
                let transition = int(d, "transition_id")?;
                if mls.pending.is_some() {
                    return Err("Overlapping DAVE transitions".into());
                }
                self.begin_transition(transition).await?;
            }
            22 => {
                let transition = int(d, "transition_id")?;
                self.mls
                    .as_mut()
                    .ok_or("DAVE execute before session")?
                    .execute(transition)?;
            }
            24 => {
                self.mls.as_ref().ok_or("DAVE epoch before session")?;
                let version = version(d, "protocol_version", self.max_version)?;
                if number(d, "epoch")? != 1 {
                    return Err("DAVE version migration unsupported".into());
                }
                self.new_key_package(version).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn on_ready(&mut self, d: &Value) -> Result<(), String> {
        if self.udp.is_some() {
            return Err("Duplicate voice Ready".into());
        }
        if !d["modes"]
            .as_array()
            .ok_or("Missing transport modes")?
            .iter()
            .any(|m| m.as_str() == Some(rtp::MODE))
        {
            return Err("AES256-GCM rtpsize unavailable".into());
        }
        self.ssrc = int(d, "ssrc")?;
        if self.ssrc == 0 {
            return Err("Invalid local SSRC".into());
        }
        if self.mode.video() {
            let streams = d["streams"]
                .as_array()
                .filter(|streams| streams.len() <= 8)
                .ok_or("Missing allocated video streams")?;
            let mut allocated = streams
                .iter()
                .filter(|stream| stream["rid"].as_str() == Some("100"));
            let stream = allocated.next().ok_or("Missing allocated screen RID")?;
            if allocated.next().is_some() {
                return Err("Duplicate allocated screen RID".into());
            }
            let video = int(stream, "ssrc")?;
            let rtx = int(stream, "rtx_ssrc")?;
            if video == 0 || rtx == 0 || video == rtx || video == self.ssrc || rtx == self.ssrc {
                return Err("Invalid allocated video SSRCs".into());
            }
            self.video_ssrc = Some((video, rtx));
        }
        let ip = d["ip"]
            .as_str()
            .ok_or("Missing voice UDP address")?
            .parse::<IpAddr>()
            .map_err(|_| "Invalid voice UDP address")?;
        let port = int(d, "port")?;
        if port == 0 || ip.is_unspecified() || ip.is_multicast() || ip.is_loopback() {
            return Err("Invalid voice UDP peer".into());
        }
        let socket = UdpSocket::bind(if ip.is_ipv4() { "0.0.0.0:0" } else { "[::]:0" })
            .await
            .map_err(|_| "Voice UDP bind failed")?;
        socket
            .connect(SocketAddr::new(ip, port))
            .await
            .map_err(|_| "Voice UDP connect failed")?;
        let mut discovery = [0; 74];
        discovery[..4].copy_from_slice(&[0, 1, 0, 70]);
        discovery[4..8].copy_from_slice(&self.ssrc.to_be_bytes());
        socket
            .send(&discovery)
            .await
            .map_err(|_| "UDP discovery send failed")?;
        self.udp = Some(socket);
        Ok(())
    }

    async fn on_session(&mut self, d: &Value) -> Result<(), String> {
        if !self.discovered || self.cipher.is_some() {
            return Err("Unexpected voice Session Description".into());
        }
        if d["mode"].as_str() != Some(rtp::MODE)
            || d["audio_codec"].as_str().is_some_and(|c| c != "opus")
        {
            return Err("Unsupported negotiated voice mode or codec".into());
        }
        if self.mode.video() {
            self.select_codec(d["video_codec"].as_str())?;
        }
        let key = d["secret_key"]
            .as_array()
            .filter(|a| a.len() == 32)
            .ok_or("Invalid transport key")?
            .iter()
            .map(|v| {
                v.as_u64()
                    .and_then(|n| u8::try_from(n).ok())
                    .ok_or("Invalid transport key")
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.cipher = Some(rtp::Cipher::new(&key)?);
        if let Mode::GoLive { settings, .. } = &self.mode {
            let capture::VideoSettings {
                width,
                height,
                fps,
                bitrate,
                ..
            } = *settings;
            let (video, rtx) = self.video_ssrc.ok_or("Missing allocated video SSRCs")?;
            // Advertise before MLS readiness so an official viewer can join.
            event(&mut self.ws, 12, json!({"audio_ssrc":self.ssrc,"video_ssrc":video,"rtx_ssrc":rtx,"streams":[{"type":"video","rid":"100","ssrc":video,"rtx_ssrc":rtx,"active":true,"quality":100,"max_bitrate":bitrate,"max_framerate":fps,"max_resolution":{"type":"fixed","width":width,"height":height}}]})).await?;
        }
        let version = version(d, "dave_protocol_version", self.max_version)?;
        self.new_key_package(version).await
    }

    /// A fresh session clears libdave's retained roster as well as the HPKE leaf.
    /// The call's shared signing identity survives independently.
    async fn new_key_package(&mut self, version: u16) -> Result<(), String> {
        let generation = self
            .mls
            .as_ref()
            .map_or(0, |mls| mls.generation.wrapping_add(1));
        let mut mls = Mls::new(
            &self.identity,
            self.group,
            version,
            self.external.as_deref(),
        )?;
        mls.generation = generation;
        let package = mls
            .session
            .key_package()
            .map_err(|_| "DAVE key package failed")?;
        self.mls = Some(mls);
        binary(&mut self.ws, 26, package).await
    }

    /// Invalidates a transition and restarts from a fresh key package.
    async fn recover(&mut self, transition: Option<u16>) -> Result<(), String> {
        if self.recoveries >= 3 {
            return Err("MLS recovery budget exhausted".into());
        }
        self.recoveries += 1;
        if let Some(transition) = transition {
            event(&mut self.ws, 31, json!({"transition_id":transition})).await?;
        }
        let version = self
            .mls
            .as_ref()
            .ok_or("DAVE missing")?
            .session
            .protocol_version();
        self.new_key_package(version).await
    }

    async fn begin_transition(&mut self, transition: u16) -> Result<(), String> {
        let mls = self.mls.as_mut().ok_or("DAVE missing")?;
        mls.pending = Some((transition, Instant::now()));
        if transition == 0 {
            mls.execute(0)
        } else {
            event(&mut self.ws, 23, json!({"transition_id":transition})).await
        }
    }

    async fn on_binary(&mut self, bytes: &[u8]) -> Result<(), String> {
        if bytes.len() < 4 {
            return Err("Malformed binary voice message".into());
        }
        self.seq_ack = i64::from(u16::from_be_bytes([bytes[0], bytes[1]]));
        let (op, data) = (bytes[2], &bytes[3..]);
        match op {
            25 => {
                if data.len() > 4096 || self.external.is_some() {
                    return Err("Invalid MLS external sender package".into());
                }
                self.external = Some(data.to_vec());
                if let Some(mls) = &mut self.mls {
                    mls.set_external_sender(data)?;
                }
                Ok(())
            }
            27 | 29 | 30 => self.on_mls(op, data).await,
            _ => Err("Unsupported binary voice opcode".into()),
        }
    }

    async fn on_mls(&mut self, op: u8, data: &[u8]) -> Result<(), String> {
        let mls = self.mls.as_ref().ok_or("MLS message before session")?;
        if !mls.external {
            return Err("MLS message before external sender".into());
        }
        let recognized: Vec<_> = self.users.iter().map(String::as_str).collect();
        let mls = self.mls.as_mut().ok_or("MLS message before session")?;
        if op == 27 {
            // MLS state advances on commit, independently of the pending media-key switch.
            if let Some(commit) = mls
                .session
                .process_proposals(data, &recognized)
                .map_err(|_| "MLS proposals rejected")?
            {
                binary(&mut self.ws, 28, commit).await?;
            }
            return Ok(());
        }
        if data.len() < 3 || mls.pending.is_some() {
            return Err("Invalid MLS transition".into());
        }
        let transition = u16::from_be_bytes([data[0], data[1]]);
        let result = if op == 29 {
            match mls.session.process_commit(&data[2..]) {
                Ok(Some(roster)) => Ok((roster, false)),
                Ok(None) => return Ok(()),
                Err(()) => Err(()),
            }
        } else {
            mls.session
                .process_welcome(&data[2..], &recognized)
                .map(|r| (r, true))
        };
        let Ok((roster, full)) = result else {
            event(&mut self.ws, 31, json!({"transition_id":transition})).await?;
            self.recover(None).await?;
            self.dave_ready = false;
            self.report();
            self.startup = Instant::now() + DEADLINE;
            return Ok(());
        };
        mls.roster(
            roster,
            full,
            &self.users,
            &self.user_id,
            matches!(self.mode, Mode::Voice { .. }),
        )?;
        self.begin_transition(transition).await
    }

    async fn update_ready(&mut self) -> Result<(), String> {
        let ready = self.mls.as_ref().is_some_and(|m| m.ready);
        if !ready && self.dave_ready {
            self.startup = Instant::now() + self.negotiation_timeout;
        }
        let transport = self.cipher.is_some() && self.discovered && self.heartbeat.is_some();
        if ready && transport && !(self.dave_ready && self.status.transport_ready) {
            let speaking = match &mut self.mode {
                Mode::Voice { speaking, .. } => Some(u8::from(*speaking.borrow_and_update())),
                Mode::GoLive { audio, .. } => {
                    let audio = audio.is_some();
                    self.request_keyframe()?;
                    audio.then_some(2)
                }
            };
            if let Some(speaking) = speaking {
                event(
                    &mut self.ws,
                    5,
                    json!({"speaking":speaking,"delay":0,"ssrc":self.ssrc}),
                )
                .await?;
            }
        }
        if ready != self.dave_ready || transport != self.status.transport_ready {
            self.dave_ready = ready;
            self.status.transport_ready = transport;
            self.report();
        }
        Ok(())
    }
}
