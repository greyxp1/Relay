use crate::WebSocket;
use crate::{
    capture::{Capture, ClockTime, Codec, Frame, ScreenSettings, VideoEncoder, VideoSettings},
    dave::Identity,
    playback::Playback,
    transport::{self, Credentials, Mode},
};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, sync::atomic::Ordering, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{self, Instant},
};
use tokio_tungstenite::tungstenite::Message;
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
    users: HashMap<u64, f64>,
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
            let _ = output.try_send(Message::Text(json!({"op":"deviceError", "error":format!("{error}. Using the system default microphone.")}).to_string().into()));
            Ok(capture)
        })
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
enum Command {
    VideoDevices,
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
    Voice {
        id: String,
        #[serde(flatten)]
        voice: Voice,
    },
    PrepareShare {
        id: String,
        source: Option<String>,
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
    source: Option<String>,
}
impl Drop for Call {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Prepared {
    source: Option<String>,
    capture: Capture,
    frames: mpsc::Receiver<Frame>,
    video: VideoSettings,
    codecs: Vec<Codec>,
    expires: Instant,
    audio: bool,
}

enum CaptureChange {
    Prepared(Prepared),
    Camera {
        source: String,
        quality: ScreenSettings,
        audio: bool,
        reply: oneshot::Sender<Result<(), String>>,
    },
}

async fn prepare(
    quality: ScreenSettings,
    audio: bool,
    source: Option<String>,
) -> Result<Prepared, String> {
    let quality = quality.validate()?;
    let encoder = if VideoEncoder::Nvenc.available(Codec::H264)? {
        VideoEncoder::Nvenc
    } else {
        VideoEncoder::X264
    };
    let camera = source
        .as_ref()
        .filter(|source| !Capture::is_screen_source(source));
    let (capture, frames, video) = if let Some(source) = camera.cloned() {
        tokio::task::spawn_blocking(move || Capture::camera(&source, encoder, Codec::H264, quality))
            .await
            .map_err(|_| "Video device setup failed")??
    } else {
        Capture::screen(encoder, Codec::H264, source.as_deref(), quality).await?
    };
    let codecs = video.encoder.codecs()?;
    Ok(Prepared {
        source,
        capture,
        frames,
        video,
        codecs,
        expires: Instant::now() + Duration::from_secs(60),
        audio,
    })
}

type Picker = (
    String,
    Option<String>,
    JoinHandle<Result<Option<Prepared>, String>>,
);

struct Controls {
    voice: watch::Receiver<Voice>,
    keyframe: watch::Sender<()>,
    capture: mpsc::Receiver<CaptureChange>,
}

async fn preparing(task: &mut Option<Picker>) -> Result<Option<Prepared>, String> {
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
    let (finished, mut completions) = mpsc::channel::<String>(16);
    let mut calls = HashMap::<String, Call>::new();
    let mut identity: Option<(String, Zeroizing<String>, Identity)> = None;
    let mut prepared = None;
    let mut picker = None;
    let result = async {
        send(&mut sending, Message::Text("{}".into())).await?;
        let mut tick = time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                message = incoming.next() => match message {
                    Some(Ok(Message::Text(text))) => {
                        let command: Command = serde_json::from_str(&text).map_err(|_| "Invalid media command")?;
                        match command {
                            Command::VideoDevices => {
                                let (devices, screens) = tokio::task::spawn_blocking(|| (Capture::video_devices(), Capture::screen_sources()))
                                    .await.map_err(|_| "Video device discovery failed")?;
                                let response = match devices {
                                    Ok(devices) => json!({"op":"videoDevices", "devices":devices, "screens":screens}),
                                    Err(error) => json!({"op":"videoDevices", "error":error, "screens":screens}),
                                };
                                send(&mut sending, Message::Text(response.to_string().into())).await?;
                            }
                            Command::Connect { id, user, session, server, channel, token, endpoint, sending: streaming, voice } => {
                                if id.is_empty() || id.len() > 64 || calls.contains_key(&id) || calls.len() >= 16 {
                                    return Err("Invalid media connection".into());
                                }
                                if let Some((expected_user, expected_session, _)) = &identity {
                                    if &user != expected_user || &session != expected_session { return Err("Account or session changed".into()); }
                                } else {
                                    identity = Some((user.clone(), session.clone(), Identity::new(&user)?));
                                }
                                let credentials = Credentials {
                                    identity: identity.as_ref().unwrap().2.clone(),
                                    user_id: user, session_id: session, server_id: server, channel_id: channel, token, endpoint,
                                };
                                let (voice_control, settings) = watch::channel(voice);
                                let (keyframe, _) = watch::channel(());
                                let capture = if streaming {
                                    match prepared.take() {
                                        Some(capture) => Some(capture),
                                        None => {
                                            send(&mut sending, Message::Text(json!({"op":"ended", "id":id, "error":"Select a screen before streaming"}).to_string().into())).await?;
                                            continue;
                                        }
                                    }
                                } else { None };
                                let source = capture.as_ref().and_then(|capture| capture.source.clone());
                                let output = output.clone();
                                let finished = finished.clone();
                                let call_id = id.clone();
                                let recovery = keyframe.clone();
                                let (replacement, replacements) = mpsc::channel(1);
                                let task = tokio::task::spawn_local(async move {
                                    let result = run(&call_id, credentials, capture, Controls { voice: settings, keyframe: recovery, capture: replacements }, &output).await;
                                    let _ = output.send(Message::Text(json!({"op":"ended", "id":call_id, "error":result.err()}).to_string().into())).await;
                                    let _ = finished.send(call_id).await;
                                });
                                calls.insert(id, Call { task, voice: voice_control, keyframe, capture: streaming.then_some(replacement), source });
                            }
                            Command::Stop { id } => { calls.remove(&id); }
                            Command::Voice { id, voice } => {
                                if let Some(call) = calls.get(&id) { call.voice.send_replace(voice); }
                            }
                            Command::Keyframe { id } => {
                                if let Some(call) = calls.get(&id) { call.keyframe.send_replace(()); }
                            }
                            Command::PrepareShare { id, source, quality, audio, replace } => {
                                if let Some((_, _, task)) = picker.take() { task.abort(); }
                                prepared = None;
                                let same_camera = source.as_ref().filter(|source| !Capture::is_screen_source(source)).and_then(|source| replace.as_ref()
                                    .and_then(|id| calls.get(id)).filter(|call| call.source.as_ref() == Some(source))
                                    .and_then(|call| call.capture.as_ref()));
                                let task = if let Some(camera) = same_camera {
                                    let camera = camera.clone();
                                    tokio::task::spawn_local(async move {
                                        let quality = quality.validate()?;
                                        let (reply, response) = oneshot::channel();
                                        camera.send(CaptureChange::Camera { source: source.unwrap(), quality, audio, reply }).await
                                            .map_err(|_| "Stream controls unavailable")?;
                                        response.await.map_err(|_| "Video device change stopped")?.map(|()| None)
                                    })
                                } else {
                                    tokio::task::spawn_local(async move { prepare(quality, audio, source).await.map(Some) })
                                };
                                picker = Some((id, replace, task));
                            }
                            Command::CancelShare => {
                                if let Some((_, _, task)) = picker.take() { task.abort(); }
                                prepared = None;
                            }
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => send(&mut sending, Message::Pong(bytes)).await?,
                    Some(Ok(Message::Close(_))) | None => break Ok(()),
                    Some(Err(_)) => break Err("Browser connection closed".into()),
                    _ => {},
                },
                Some(message) = outgoing.recv() => {
                    send(&mut sending, message).await?;
                }
                Some(id) = completions.recv() => { calls.remove(&id); }
                result = preparing(&mut picker) => {
                    let (id, replace, _) = picker.take().unwrap();
                    let error = match result {
                        Ok(None) => None,
                        Ok(Some(value)) => if let Some(target) = replace {
                            let source = value.source.clone();
                            calls.get_mut(&target).and_then(|call| {
                                call.capture.as_ref()?.try_send(CaptureChange::Prepared(value)).ok()?;
                                call.source = source;
                                Some(())
                            }).is_none().then(|| "Stream ended before the source changed".to_owned())
                        } else { prepared = Some(value); None },
                        Err(e) => Some(e),
                    };
                    send(&mut sending, Message::Text(json!({"op":"prepared", "id":id, "error":error}).to_string().into())).await?;
                }
                frame = preview(&mut prepared) => {
                    if let Some(frame) = frame {
                        if let Some((user, _, _)) = &identity {
                            let user = user.parse().map_err(|_| "Invalid user")?;
                            let _ = output.try_reserve().map(|permit| permit.send(preview_frame(user, &frame)));
                        }
                    } else { prepared = None; }
                }
                _ = tick.tick() => {
                    let capture_error = prepared.as_ref().and_then(|p| p.capture.error()
                        .or_else(|| (p.expires <= Instant::now()).then(|| "Screen selection expired".into())));
                    if let Some(error) = capture_error {
                        prepared = None;
                        let _ = output.try_send(Message::Text(json!({"op":"captureEnded", "error":error}).to_string().into()));
                    }
                }
            }
        }
    }.await;
    if let Some((_, _, task)) = picker {
        task.abort();
    }
    drop(calls);
    result
}

async fn run(
    id: &str,
    credentials: Credentials,
    mut prepared: Option<Prepared>,
    mut controls: Controls,
    output: &mpsc::Sender<Message>,
) -> Result<(), String> {
    let streaming = prepared.is_some();
    loop {
        let next = {
            let session = run_session(
                id,
                credentials.clone(),
                prepared.take(),
                controls.voice.clone(),
                controls.keyframe.clone(),
                output,
            );
            tokio::pin!(session);
            tokio::select! {
                result = &mut session => return result,
                replacement = controls.capture.recv(), if streaming => replacement.ok_or("Stream controls closed")?,
            }
        };
        // The Discord stream stays open while its media session adopts the new source and quality.
        output
            .try_reserve()
            .map_err(|_| "Browser playback stalled")?
            .send(packet(
                4,
                0,
                credentials.user_id.parse().map_err(|_| "Invalid user")?,
                0,
                &[],
            ));
        prepared = Some(match next {
            CaptureChange::Prepared(prepared) => prepared,
            CaptureChange::Camera {
                source,
                quality,
                audio,
                reply,
            } => {
                // Cameras can be exclusive. The old session must release its device before reopening it.
                match prepare(quality, audio, Some(source)).await {
                    Ok(prepared) => {
                        let _ = reply.send(Ok(()));
                        prepared
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error.clone()));
                        return Err(error);
                    }
                }
            }
        });
    }
}

async fn run_session(
    id: &str,
    credentials: Credentials,
    prepared: Option<Prepared>,
    mut settings: watch::Receiver<Voice>,
    requests: watch::Sender<()>,
    output: &mpsc::Sender<Message>,
) -> Result<(), String> {
    let (status_tx, mut status_rx) = watch::channel(transport::Status::default());
    let report = move |status| {
        status_tx.send_if_modified(|previous| std::mem::replace(previous, status) != status);
    };
    let (received, mut frames) = mpsc::channel(16);
    let (video, mut pictures) = mpsc::channel(4);
    let mut current = settings.borrow_and_update().clone();
    let mut capture;
    let mut audio_capture = None;
    let mut playback = None;
    let mut capture_frames;
    let (codec_tx, mut codec_rx) = watch::channel(Codec::H264);
    let (keyframe_tx, mut keyframe_rx) = mpsc::channel(1);
    let (voice_tx, voice_rx) = watch::channel(!current.mute && !current.deaf);
    let (send, input) = mpsc::channel(4);
    let user: u64 = credentials.user_id.parse().map_err(|_| "Invalid user")?;
    let mode = if let Some(p) = prepared {
        capture = p.capture;
        capture_frames = p.frames;
        let audio_input = if p.audio {
            let (audio, input) = Capture::share_audio().await?;
            audio_capture = Some(audio);
            Some(input)
        } else {
            None
        };
        Mode::GoLive {
            settings: p.video,
            codecs: p.codecs,
            codec: codec_tx,
            video: input,
            audio: audio_input,
            keyframes: keyframe_tx,
        }
    } else {
        let source = current.source.clone();
        let volume = current.volume.unwrap_or(1.0);
        let device = current.output.clone();
        let output = output.clone();
        let ((mic, receiver), mut native) = tokio::task::spawn_blocking(move || {
            let capture = microphone(source.as_deref(), &output)?;
            capture.0.set_audio_volume(volume)?;
            let playback = Playback::new(&device).or_else(|error| {
                let _ = output.try_send(Message::Text(json!({"op":"deviceError", "error":format!("{error}. Using the system default speaker.")}).to_string().into()));
                Playback::new("")
            })?;
            Ok::<_, String>((capture, playback))
        })
        .await
        .map_err(|_| "Microphone setup failed")??;
        native.set_volume(
            if current.deaf {
                0.0
            } else {
                current.output_volume
            },
            &current.users,
        );
        playback = Some(native);
        capture = mic;
        capture_frames = receiver;
        Mode::Voice {
            video,
            keyframes: requests.subscribe(),
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
    let mut recovery_requests = requests.subscribe();
    let mut heard = None;
    let mut tick = time::interval(Duration::from_millis(100));
    loop {
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
                if let Err(mpsc::error::TrySendError::Closed(_)) = output.try_send(Message::Text(json!({"op":"state", "id":id, "ready":status.transport_ready, "ping":status.ping}).to_string().into())) {
                    return Err("Browser connection closed".into());
                }
            }
            changed = settings.changed(), if !streaming => {
                changed.map_err(|_| "Voice controls closed")?;
                let next = settings.borrow_and_update().clone();
                if next.source != current.source {
                    let source = next.source.clone();
                    let output_tx = output.clone();
                    let replacement = tokio::task::spawn_blocking(move || {
                        microphone(source.as_deref(), &output_tx)
                    }).await.map_err(|_| "Microphone setup failed")?;
                    match replacement {
                        Ok((replacement, receiver)) => {
                            offset = ClockTime::from_nseconds(started.elapsed().as_nanos() as u64).max(last_pts + ClockTime::from_mseconds(20));
                            capture = replacement; capture_frames = receiver;
                        }
                        Err(error) => {
                            let _ = output.try_send(Message::Text(json!({"op":"deviceError", "error":format!("{error}. Previous microphone remains active.")}).to_string().into()));
                        }
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
                            let _ = output.try_send(Message::Text(json!({"op":"deviceError", "error":error}).to_string().into()));
                        }
                    }
                    native.set_volume(if next.deaf { 0.0 } else { next.output_volume }, &next.users);
                    playback = Some(native);
                }
                // Remember the request so mute/volume updates do not reopen unavailable hardware.
                current = next;
                voice_tx.send_replace(!current.mute && !current.deaf);
            }
            permit = send.reserve(), if streaming && pending_frame.is_some() => {
                permit.map_err(|_| "Media sender closed")?.send(pending_frame.take().unwrap());
            }
            frame = capture_frames.recv(), if !streaming || pending_frame.is_none() => {
                let mut frame = frame.ok_or("Capture ended")?;
                if streaming {
                    if !preview_waiting || frame.keyframe {
                        preview_waiting = output.try_reserve().map(|permit| permit.send(preview_frame(user, &frame))).is_err();
                        if preview_waiting { capture.request_keyframe()?; }
                    }
                } else {
                    if !*voice_tx.borrow() { continue; }
                    frame.pts += offset; last_pts = frame.pts;
                }
                if streaming { pending_frame = Some(frame); }
                else if let Err(mpsc::error::TrySendError::Closed(_)) = send.try_send(frame) { return Err("Media sender closed".into()); }
            }
            frame = frames.recv(), if !streaming => {
                let frame = frame.ok_or("Media receiver closed")?;
                playback.as_mut().unwrap().push(frame)?;
            }
            Some((user, frame)) = pictures.recv(), if !streaming => {
                if let Some(frame) = frame {
                    if !frame.valid() { continue; }
                    let timestamp = ((u128::from(frame.timestamp) << 32) / 1_000_000_000) as u64;
                    if output.try_reserve().map(|permit| permit.send(packet(5, flags(Codec::H264, frame.keyframe), user, timestamp, &frame.data))).is_ok() {
                        frame.delivered();
                    } else {
                        frame.lost();
                        requests.send_replace(());
                    }
                } else {
                    let _ = output.try_send(Message::Text(json!({"op":"cameraEnded", "user":user.to_string()}).to_string().into()));
                }
            }
            Ok(()) = codec_rx.changed(), if streaming => {
                pending_frame = None;
                capture.set_codec(*codec_rx.borrow_and_update())?;
                preview_waiting = true;
                let _ = output.try_reserve().map(|permit| permit.send(packet(4, 0, user, 0, &[])));
            }
            Ok(()) = recovery_requests.changed(), if streaming => {
                preview_waiting = true;
                capture.request_keyframe()?;
            }
            Some(()) = keyframe_rx.recv(), if streaming => { capture.request_keyframe()?; }
            _ = tick.tick() => {
                if let Some(error) = capture.error().or_else(|| audio_capture.as_ref().and_then(Capture::error)) { return Err(error); }
                if !streaming {
                    let native = playback.as_mut().unwrap();
                    if let Some(error) = native.error() { return Err(error); }
                    let mut speakers = native.speakers();
                    if capture.peak.swap(0, Ordering::Relaxed) > 512 { heard = Some(Instant::now()); }
                    let speaking = *voice_tx.borrow() && heard.is_some_and(|t| t.elapsed() < Duration::from_millis(300));
                    if speaking { speakers.push(user); }
                    speakers.sort_unstable();
                    if speakers != shown && output.try_send(Message::Text(json!({"op":"speakers", "users":speakers.iter().map(u64::to_string).collect::<Vec<_>>(), "id":id}).to_string().into())).is_ok() { shown = speakers; }
                }
            }
        }
    }
}

fn packet(kind: u8, flags: u8, user: u64, timestamp: u64, data: &[u8]) -> Message {
    let mut bytes = Vec::with_capacity(18 + data.len());
    bytes.extend_from_slice(&[kind, flags]);
    bytes.extend_from_slice(&user.to_be_bytes());
    bytes.extend_from_slice(&timestamp.to_be_bytes());
    bytes.extend_from_slice(data);
    Message::Binary(bytes.into())
}
fn flags(codec: Codec, keyframe: bool) -> u8 {
    u8::from(keyframe)
        | match codec {
            Codec::H265 => 2,
            Codec::Av1 => 4,
            _ => 0,
        }
}
fn preview_frame(user: u64, frame: &Frame) -> Message {
    packet(
        2,
        flags(frame.codec, frame.keyframe),
        user,
        ((u128::from(frame.pts.nseconds()) << 32) / 1_000_000_000) as u64,
        &frame.data,
    )
}
