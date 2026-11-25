use crate::WebSocket;
use crate::{
    capture::{Capture, ClockTime, Codec, Frame, ScreenSettings, VideoSettings},
    dave::Identity,
    playback::{Playback, STREAM_AUDIO_BYTES},
    transport::{self, Credentials, Mode},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::HashMap, sync::atomic::Ordering, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_tungstenite::tungstenite::{Bytes, Message};
use zeroize::Zeroizing;

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Voice {
    source: Option<String>,
    volume: Option<f64>,
    #[serde(default)]
    mute: bool,
    #[serde(default)]
    deaf: bool,
    output: String,
    output_volume: f64,
    #[serde(deserialize_with = "user_volumes")]
    users: HashMap<u64, f64>,
}

impl Voice {
    fn speaker_volume(&self) -> f64 {
        if self.deaf {
            0.0
        } else {
            self.output_volume
        }
    }
}

fn user_volumes<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<HashMap<u64, f64>, D::Error> {
    // Flattened fields lose serde_json's conversion of object keys to numeric IDs.
    HashMap::<String, f64>::deserialize(deserializer)?
        .into_iter()
        .map(|(user, volume)| Ok((user.parse().map_err(serde::de::Error::custom)?, volume)))
        .collect()
}

fn text(value: Value) -> Message {
    Message::Text(value.to_string().into())
}

fn device_error(output: &mpsc::Sender<Message>, error: String) {
    let _ = output.try_send(text(json!({"op":"deviceError", "error":error})));
}

fn microphone(
    source: Option<&str>,
    output: &mpsc::Sender<Message>,
) -> Result<(Capture, mpsc::Receiver<Frame>), String> {
    let Some(source) = source else {
        return Capture::audio("");
    };
    Capture::audio_device(source, false)
        .and_then(|device| Capture::audio(&device))
        .or_else(|error| {
            let capture = Capture::audio("")?;
            device_error(
                output,
                format!("{error}. Using the system default microphone."),
            );
            Ok(capture)
        })
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Command {
    Connect {
        id: String,
        user: String,
        session: Zeroizing<String>,
        server: String,
        channel: String,
        token: Zeroizing<String>,
        endpoint: String,
        #[serde(default)]
        sending: bool,
        #[serde(flatten)]
        voice: Voice,
    },
    Stop {
        id: String,
    },
    Reconnect {
        id: String,
        endpoint: String,
        token: Zeroizing<String>,
    },
    Voice {
        id: String,
        #[serde(flatten)]
        voice: Voice,
    },
    PrepareShare {
        id: String,
        #[serde(flatten)]
        quality: ScreenSettings,
        audio: bool,
        replace: Option<String>,
    },
    CancelShare,
    Keyframe {
        id: String,
    },
}

struct Call {
    task: JoinHandle<()>,
    voice: watch::Sender<Voice>,
    keyframe: watch::Sender<()>,
    capture: Option<mpsc::Sender<CaptureChange>>,
}
impl Drop for Call {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Prepared {
    capture: Capture,
    frames: mpsc::Receiver<Frame>,
    video: VideoSettings,
    codecs: Vec<Codec>,
    expires: Instant,
    audio: bool,
}

enum CaptureChange {
    Prepared(Prepared),
    Reconnect {
        endpoint: String,
        token: Zeroizing<String>,
    },
}

async fn prepare(quality: ScreenSettings, audio: bool) -> Result<Prepared, String> {
    let (capture, frames, video) = Capture::screen(Codec::H264, quality).await?;
    let codecs = Codec::available()?;
    Ok(Prepared {
        capture,
        frames,
        video,
        codecs,
        expires: Instant::now() + Duration::from_secs(60),
        audio,
    })
}

type Picker = (String, Option<String>, JoinHandle<Result<Prepared, String>>);

async fn preparing(task: &mut Option<Picker>) -> Result<Prepared, String> {
    match task {
        Some((_, _, task)) => task.await.map_err(|_| "Screen picker stopped".to_owned())?,
        None => std::future::pending().await,
    }
}

async fn preview(prepared: &mut Option<Prepared>) -> Option<Frame> {
    match prepared {
        Some(p) => p.frames.recv().await,
        None => std::future::pending().await,
    }
}

async fn send(
    socket: &mut futures_util::stream::SplitSink<WebSocket, Message>,
    message: Message,
) -> Result<(), String> {
    time::timeout(Duration::from_secs(2), socket.send(message))
        .await
        .map_err(|_| "Browser playback stalled")?
        .map_err(|_| "Browser connection closed".into())
}

pub async fn serve(socket: WebSocket) -> Result<(), String> {
    let (mut sending, mut incoming) = socket.split();
    let (output, mut outgoing) = mpsc::channel(64);
    let (finished, mut completions) =
        mpsc::channel::<(String, Option<Prepared>, Option<String>)>(16);
    let mut calls = HashMap::<String, Call>::new();
    let mut identity: Option<(String, Zeroizing<String>, Identity)> = None;
    let mut prepared: Option<Prepared> = None;
    let mut picker: Option<Picker> = None;
    let mut stream_audio = None;
    let result = async {
        send(&mut sending, Message::Text("{}".into())).await?;
        let mut tick = time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                message = incoming.next() => match message {
                    Some(Ok(Message::Text(message))) => {
                        match serde_json::from_str(&message).map_err(|_| "Invalid media command")? {
                            Command::Connect { id, user, session, server, channel, token, endpoint, sending: streaming, voice } => {
                                if id.is_empty() || id.len() > 64 || calls.contains_key(&id) || calls.len() >= 16 {
                                    return Err("Invalid media connection".into());
                                }
                                match &identity {
                                    Some((known, known_session, _)) if (known, known_session) != (&user, &session) => {
                                        return Err("Account or session changed".into());
                                    }
                                    Some(_) => {}
                                    None => identity = Some((user.clone(), session.clone(), Identity::new(&user)?)),
                                }
                                let capture = match (streaming, prepared.take()) {
                                    (false, kept) => { prepared = kept; None }
                                    (true, Some(capture)) => Some(capture),
                                    (true, None) => {
                                        send(&mut sending, text(json!({"op":"ended", "id":id, "error":"Select a screen before streaming"}))).await?;
                                        continue;
                                    }
                                };
                                let credentials = Credentials {
                                    identity: identity.as_ref().unwrap().2.clone(),
                                    user_id: user, session_id: session, server_id: server, channel_id: channel, token, endpoint,
                                };
                                let (voice, settings) = watch::channel(voice);
                                let (keyframe, _) = watch::channel(());
                                let (replacement, replacements) = mpsc::channel(1);
                                let (audio, stream_samples) = mpsc::channel(3);
                                if !streaming {
                                    stream_audio = Some(audio);
                                }
                                let (output, finished, call_id, keyframes) = (output.clone(), finished.clone(), id.clone(), keyframe.clone());
                                let task = tokio::task::spawn_local(async move {
                                    let controls = Controls { voice: settings, keyframe: keyframes, capture: replacements, stream_samples };
                                    let (result, recovered) = run(credentials, capture, controls, &call_id, &output).await;
                                    let error = result.err();
                                    if let Some(error) = &error {
                                        eprintln!("Call {call_id} ended: {error}");
                                    }
                                    let _ = finished.send((call_id, recovered, error)).await;
                                });
                                calls.insert(id, Call { task, voice, keyframe, capture: streaming.then_some(replacement) });
                            }
                            Command::Stop { id } => { calls.remove(&id); }
                            Command::Reconnect { id, endpoint, token } => {
                                if let Some(capture) = calls.get(&id).and_then(|call| call.capture.as_ref()) {
                                    let _ = capture.try_send(CaptureChange::Reconnect { endpoint, token });
                                }
                            }
                            Command::Voice { id, voice } => {
                                if let Some(call) = calls.get(&id) { call.voice.send_replace(voice); }
                            }
                            Command::Keyframe { id } => {
                                if let Some(call) = calls.get(&id) { call.keyframe.send_replace(()); }
                            }
                            Command::PrepareShare { id, quality, audio, replace } => {
                                if let Some((_, _, task)) = picker.take() { task.abort(); }
                                prepared = None;
                                picker = Some((id, replace, tokio::task::spawn_local(prepare(quality, audio))));
                            }
                            Command::CancelShare => {
                                if let Some((_, _, task)) = picker.take() { task.abort(); }
                                prepared = None;
                            }
                        }
                    }
                    Some(Ok(Message::Binary(samples))) if samples.len() == STREAM_AUDIO_BYTES => {
                        if let Some(audio) = stream_audio.as_ref() {
                            let _ = audio.try_send(samples);
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => send(&mut sending, Message::Pong(bytes)).await?,
                    Some(Ok(Message::Close(_))) | None => break Ok(()),
                    Some(Err(_)) => break Err("Browser connection closed".into()),
                    _ => {},
                },
                Some(message) = outgoing.recv() => send(&mut sending, message).await?,
                Some((id, recovered, error)) = completions.recv() => {
                    calls.remove(&id);
                    let recoverable = recovered.is_some() && prepared.is_none();
                    if let Some(mut recovered) = recovered.filter(|_| recoverable) {
                        recovered.expires = Instant::now() + Duration::from_secs(60);
                        prepared = Some(recovered);
                    }
                    send(&mut sending, text(json!({"op":"ended", "id":id, "error":error, "recoverable":recoverable}))).await?;
                }
                result = preparing(&mut picker) => {
                    let (id, replace, _) = picker.take().unwrap();
                    let error = match (result, replace) {
                        (Ok(value), Some(target)) => calls
                            .get(&target)
                            .and_then(|call| call.capture.as_ref()?.try_send(CaptureChange::Prepared(value)).ok())
                            .is_none()
                            .then(|| "Stream ended before the source changed".to_owned()),
                        (Ok(value), None) => { prepared = Some(value); None }
                        (Err(error), _) => Some(error),
                    };
                    send(&mut sending, text(json!({"op":"prepared", "id":id, "error":error}))).await?;
                }
                frame = preview(&mut prepared) => match (frame, &identity) {
                    (Some(frame), Some((user, _, _))) => {
                        let user = user.parse().map_err(|_| "Invalid user")?;
                        let _ = output.try_reserve().map(|permit| permit.send(preview_frame(user, &frame)));
                    }
                    (Some(_), None) => {}
                    (None, _) => prepared = None,
                },
                _ = tick.tick() => {
                    let capture_error = prepared.as_ref().and_then(|p| p.capture.error()
                        .or_else(|| (p.expires <= Instant::now()).then(|| "Screen selection expired".into())));
                    if let Some(error) = capture_error {
                        prepared = None;
                        let _ = output.try_send(text(json!({"op":"captureEnded", "error":error})));
                    }
                }
            }
        }
    }
    .await;
    if let Some((_, _, task)) = picker {
        task.abort();
    }
    drop(calls);
    result
}

struct Controls {
    voice: watch::Receiver<Voice>,
    keyframe: watch::Sender<()>,
    capture: mpsc::Receiver<CaptureChange>,
    stream_samples: mpsc::Receiver<Bytes>,
}

/// Runs media sessions until one ends, adopting replacement captures and
/// endpoints without ending the Discord stream. A failed stream returns its
/// capture when it can be reused.
async fn run(
    mut credentials: Credentials,
    mut prepared: Option<Prepared>,
    mut controls: Controls,
    id: &str,
    output: &mpsc::Sender<Message>,
) -> (Result<(), String>, Option<Prepared>) {
    let streaming = prepared.is_some();
    loop {
        let outcome = {
            let voice = controls.voice.clone();
            let session = run_session(
                id,
                credentials.clone(),
                &mut prepared,
                voice,
                &controls.keyframe,
                &mut controls.stream_samples,
                output,
            );
            tokio::select! {
                result = session => Ok(result),
                change = controls.capture.recv(), if streaming => Err(change),
            }
        };
        let change = match outcome {
            Ok(result) => {
                let recoverable = result.is_err()
                    && prepared
                        .as_ref()
                        .is_some_and(|p| p.capture.error().is_none())
                    && !matches!(
                        result.as_ref().map_err(String::as_str),
                        Err("Capture ended" | "Browser connection closed")
                    );
                return (result, prepared.filter(|_| recoverable));
            }
            Err(change) => change,
        };
        match change {
            None => return (Err("Stream controls closed".into()), None),
            Some(CaptureChange::Reconnect { endpoint, token }) => {
                credentials.endpoint = endpoint;
                credentials.token = token;
            }
            // The Discord stream stays open while its media session adopts the new source and quality.
            Some(CaptureChange::Prepared(next)) => {
                let Ok(permit) = output.try_reserve() else {
                    return (Err("Browser playback stalled".into()), None);
                };
                permit.send(packet(
                    4,
                    0,
                    credentials.user_id.parse().unwrap_or_default(),
                    0,
                    &[],
                ));
                prepared = Some(next);
            }
        }
    }
}

async fn run_session(
    id: &str,
    credentials: Credentials,
    prepared: &mut Option<Prepared>,
    mut settings: watch::Receiver<Voice>,
    keyframe: &watch::Sender<()>,
    stream_samples: &mut mpsc::Receiver<Bytes>,
    output: &mpsc::Sender<Message>,
) -> Result<(), String> {
    let (status_tx, mut status_rx) = watch::channel(transport::Status::default());
    let report = move |status| {
        status_tx.send_if_modified(|previous| std::mem::replace(previous, status) != status);
    };
    let mut current = settings.borrow_and_update().clone();
    let (received, mut frames) = mpsc::channel(16);
    let (codec_tx, mut codec_rx) =
        watch::channel(prepared.as_ref().map_or(Codec::H264, |p| p.video.codec));
    let (keyframe_tx, mut keyframe_rx) = mpsc::channel(1);
    let (voice_tx, voice_rx) = watch::channel(!current.mute && !current.deaf);
    let (send, input) = mpsc::channel(4);
    let user: u64 = credentials.user_id.parse().map_err(|_| "Invalid user")?;
    let mut voice_capture = None;
    let mut audio_capture = None;
    let mut playback = None;
    let mode = if let Some(p) = prepared.as_ref() {
        let audio = if p.audio {
            let (capture, input) = Capture::share_audio().await?;
            audio_capture = Some(capture);
            Some(input)
        } else {
            None
        };
        let codecs = p.codecs.clone();
        Mode::GoLive {
            settings: p.video,
            codecs,
            codec: codec_tx,
            video: input,
            audio,
            keyframes: keyframe_tx,
        }
    } else {
        let (source, volume, device) = (
            current.source.clone(),
            current.volume.unwrap_or(1.0),
            current.output.clone(),
        );
        let output = output.clone();
        let (mic, mut native) = tokio::task::spawn_blocking(move || {
            let capture = microphone(source.as_deref(), &output)?;
            capture.0.set_audio_volume(volume)?;
            let playback = Playback::new(&device).or_else(|error| {
                if device.is_empty() {
                    return Err(error);
                }
                device_error(
                    &output,
                    format!("{error}. Using the system default speaker."),
                );
                Playback::new("")
            })?;
            Ok::<_, String>((capture, playback))
        })
        .await
        .map_err(|_| "Microphone setup failed")??;
        native.set_volume(current.speaker_volume(), &current.users);
        playback = Some(native);
        voice_capture = Some(mic);
        Mode::Voice {
            input,
            output: received,
            speaking: voice_rx,
        }
    };
    let streaming = matches!(&mode, Mode::GoLive { .. });
    let transport = transport::run(credentials, mode, report);
    tokio::pin!(transport);
    let started = Instant::now();
    let mut offset = ClockTime::ZERO;
    let mut last_pts = ClockTime::ZERO;
    let mut shown = Vec::new();
    let mut preview_waiting = false;
    let mut pending_frame = None;
    let mut bitrate = 0;
    let mut recovery_requests = keyframe.subscribe();
    let mut heard = None;
    let mut tick = time::interval(Duration::from_millis(100));
    let preview = |frame: &Frame| {
        output
            .try_reserve()
            .map(|permit| permit.send(preview_frame(user, frame)))
            .is_ok()
    };
    loop {
        let (capture, capture_frames) = match prepared.as_mut() {
            Some(p) => (&mut p.capture, &mut p.frames),
            None => voice_capture
                .as_mut()
                .map(|(mic, frames)| (mic, frames))
                .unwrap(),
        };
        tokio::select! {
            biased;
            result = &mut transport => return result,
            changed = status_rx.changed() => {
                changed.map_err(|_| "Media status closed")?;
                let status = *status_rx.borrow_and_update();
                if streaming && status.video_bitrate != 0 && status.video_bitrate != bitrate {
                    bitrate = status.video_bitrate;
                    capture.set_video_bitrate(bitrate)?;
                }
                let state = text(json!({"op":"state", "id":id, "ready":status.transport_ready, "ping":status.ping}));
                if let Err(mpsc::error::TrySendError::Closed(_)) = output.try_send(state) {
                    return Err("Browser connection closed".into());
                }
            }
            changed = settings.changed(), if !streaming => {
                changed.map_err(|_| "Voice controls closed")?;
                let next = settings.borrow_and_update().clone();
                if next.source != current.source {
                    let (source, notices) = (next.source.clone(), output.clone());
                    let replacement = tokio::task::spawn_blocking(move || microphone(source.as_deref(), &notices))
                        .await
                        .map_err(|_| "Microphone setup failed")?;
                    match replacement {
                        Ok((replacement, receiver)) => {
                            let now = ClockTime::from_nseconds(started.elapsed().as_nanos() as u64);
                            offset = now.max(last_pts + ClockTime::from_mseconds(20));
                            *capture = replacement;
                            *capture_frames = receiver;
                        }
                        Err(error) => device_error(output, format!("{error}. Previous microphone remains active.")),
                    }
                }
                capture.set_audio_volume(next.volume.unwrap_or(1.0))?;
                if let Some(mut native) = playback.take() {
                    if next.output != current.output {
                        let device = next.output.clone();
                        let (returned, result) = tokio::task::spawn_blocking(move || {
                            let result = native.set_device(&device);
                            (native, result)
                        }).await.map_err(|_| "Speaker change failed")?;
                        native = returned;
                        if let Err(error) = result {
                            device_error(output, error);
                        }
                    }
                    native.set_volume(next.speaker_volume(), &next.users);
                    playback = Some(native);
                }
                // Remember the request so mute/volume updates do not reopen unavailable hardware.
                current = next;
                voice_tx.send_replace(!current.mute && !current.deaf);
            }
            permit = send.reserve(), if pending_frame.is_some() => {
                permit.map_err(|_| "Media sender closed")?.send(pending_frame.take().unwrap());
            }
            frame = capture_frames.recv(), if pending_frame.is_none() => {
                let mut frame = frame.ok_or("Capture ended")?;
                if streaming {
                    if !preview_waiting || frame.keyframe {
                        preview_waiting = !preview(&frame);
                        if preview_waiting { let _ = capture.request_keyframe(); }
                    }
                    pending_frame = Some(frame);
                } else if *voice_tx.borrow() {
                    frame.pts += offset;
                    last_pts = frame.pts;
                    if let Err(mpsc::error::TrySendError::Closed(_)) = send.try_send(frame) {
                        return Err("Media sender closed".into());
                    }
                }
            }
            frame = frames.recv(), if !streaming => {
                playback.as_mut().unwrap().push(frame.ok_or("Media receiver closed")?)?;
            }
            Some(samples) = stream_samples.recv(), if !streaming => {
                if let Err(error) = playback.as_mut().unwrap().push_stream(samples).await {
                    device_error(output, format!("Could not play received stream audio. {error}"));
                }
            }
            Ok(()) = codec_rx.changed(), if streaming => {
                pending_frame = None;
                let codec = *codec_rx.borrow_and_update();
                capture.set_codec(codec)?;
                if let Some(p) = prepared.as_mut() {
                    p.video.codec = codec;
                }
                preview_waiting = true;
                let _ = output.try_reserve().map(|permit| permit.send(packet(4, 0, user, 0, &[])));
            }
            Ok(()) = recovery_requests.changed(), if streaming => {
                preview_waiting = true;
                let _ = capture.request_keyframe();
            }
            Some(()) = keyframe_rx.recv(), if streaming => { let _ = capture.request_keyframe(); }
            _ = tick.tick() => {
                if let Some(error) = capture.error().or_else(|| audio_capture.as_ref().and_then(Capture::error)) {
                    return Err(error);
                }
                if let Some(native) = playback.as_mut() {
                    if let Some(error) = native.error() { return Err(error); }
                    if let Some(error) = native.stream_error() {
                        device_error(output, format!("Could not play received stream audio. {error}"));
                    }
                    let mut speakers = native.speakers();
                    if capture.peak.swap(0, Ordering::Relaxed) > 512 { heard = Some(Instant::now()); }
                    if *voice_tx.borrow() && heard.is_some_and(|t| t.elapsed() < Duration::from_millis(300)) {
                        speakers.push(user);
                    }
                    speakers.sort_unstable();
                    let users: Vec<_> = speakers.iter().map(u64::to_string).collect();
                    if speakers != shown && output.try_send(text(json!({"op":"speakers", "users":users, "id":id}))).is_ok() {
                        shown = speakers;
                    }
                }
            }
        }
    }
}

/// A binary browser message: kind, flags, user, 32.32 fixed-point seconds, then data.
fn packet(kind: u8, flags: u8, user: u64, timestamp: u64, data: &[u8]) -> Message {
    let mut bytes = Vec::with_capacity(18 + data.len());
    bytes.extend_from_slice(&[kind, flags]);
    bytes.extend_from_slice(&user.to_be_bytes());
    bytes.extend_from_slice(&timestamp.to_be_bytes());
    bytes.extend_from_slice(data);
    Message::Binary(bytes.into())
}

fn preview_frame(user: u64, frame: &Frame) -> Message {
    let codec = match frame.codec {
        Codec::H265 => 2,
        Codec::Av1 => 4,
        _ => 0,
    };
    let timestamp = ((u128::from(frame.pts.nseconds()) << 32) / 1_000_000_000) as u64;
    packet(
        2,
        codec | u8::from(frame.keyframe),
        user,
        timestamp,
        &frame.data,
    )
}
