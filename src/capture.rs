pub use gst::ClockTime;
use gstreamer::{self as gst, prelude::*};
use gstreamer_app::{AppSink, AppSinkCallbacks};
use std::{
    collections::VecDeque,
    ffi::{c_char, c_void, CString},
    sync::{
        atomic::{AtomicU16, AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::mpsc;

const AUDIO_BITRATE: u32 = 128_000;
pub(crate) const VIDEO_MIN_BITRATE: u32 = 100_000;
pub(crate) const VIDEO_START_BITRATE: u32 = 2_500_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    Opus,
    /// Annex B byte-stream, one access unit per frame.
    H264,
    /// Annex B byte-stream, one access unit per frame.
    H265,
    /// Low-overhead OBU stream, one temporal unit per frame.
    Av1,
}

impl Codec {
    /// RTP clock rate in ticks per second.
    pub(crate) fn clock(self) -> u32 {
        match self {
            Self::Opus => 48_000,
            _ => 90_000,
        }
    }

    fn encoder(self) -> &'static str {
        match self {
            Self::H265 => "nvh265enc",
            Self::Av1 => "nvav1enc",
            _ => "nvh264enc",
        }
    }

    /// Video codecs this machine encodes, most efficient first.
    /// NVENC elements register only for codecs the GPU supports.
    pub fn available() -> Result<Vec<Self>, String> {
        gst::init().map_err(|e| e.to_string())?;
        let codecs = [Self::Av1, Self::H265, Self::H264];
        Ok(codecs
            .into_iter()
            .filter(|c| gst::ElementFactory::find(c.encoder()).is_some())
            .collect())
    }
}

/// NVENC bitrate properties count kbit as 1024 bits.
fn kbits(bitrate: u32) -> u32 {
    bitrate / 1024
}

#[derive(Clone, Copy, Debug)]
pub struct VideoSettings {
    pub codec: Codec,
    pub width: u32,
    pub height: u32,
    /// Most frames per second. Static screens send fewer.
    pub fps: u32,
    /// Target bits per second, shared with the stream announcement.
    pub bitrate: u32,
}

#[derive(Clone, Copy, serde::Deserialize)]
pub struct ScreenSettings {
    pub height: u32,
    pub fps: u32,
}

impl ScreenSettings {
    pub fn validate(self) -> Result<Self, String> {
        if (self.height != 0 && !(128..=2160).contains(&self.height))
            || !(1..=240).contains(&self.fps)
        {
            return Err("Select source resolution or 128–2160p, and 1–240 fps".into());
        }
        Ok(self)
    }
}

impl VideoSettings {
    pub fn validate(self) -> Result<Self, String> {
        if !(192..=3840).contains(&self.width)
            || !self.width.is_multiple_of(2)
            || !(128..=2160).contains(&self.height)
            || !self.height.is_multiple_of(2)
            || !(1..=240).contains(&self.fps)
            || !(VIDEO_MIN_BITRATE..=50_000_000).contains(&self.bitrate)
            || !self.bitrate.is_multiple_of(1000)
            || self.codec == Codec::Opus
        {
            return Err("NVENC video requires even dimensions from 192×128 to 3840×2160, 1–240 fps, and 100000–50000000 bit/s in steps of 1000".into());
        }
        Ok(self)
    }
}

pub struct Frame {
    pub codec: Codec,
    pub pts: gst::ClockTime,
    pub data: gst::MappedBuffer<gst::buffer::Readable>,
    pub keyframe: bool,
}

unsafe extern "C" {
    fn relay_share_audio_open(name: *const c_char, result: *mut *mut c_void) -> i32;
    fn relay_share_audio_error(router: *mut c_void) -> i32;
    fn relay_share_audio_free(router: *mut c_void);
}

/// Routes other applications' audio into a private sink, without call playback.
struct ShareAudio(*mut c_void);

// All graph access is protected by PipeWire's thread loop lock, including destruction.
unsafe impl Send for ShareAudio {}

impl Drop for ShareAudio {
    fn drop(&mut self) {
        unsafe { relay_share_audio_free(self.0) }
    }
}

struct Portal {
    session: Option<ashpd::desktop::Session<ashpd::desktop::screencast::Screencast>>,
    _remote: Option<std::os::fd::OwnedFd>,
}

impl Drop for Portal {
    fn drop(&mut self) {
        if let (Some(session), Ok(runtime)) =
            (self.session.take(), tokio::runtime::Handle::try_current())
        {
            runtime.spawn(async move {
                let _ = session.close().await;
            });
        }
    }
}

/// Owns the pipeline and its callbacks. Dropping it stops capture synchronously.
pub struct Capture {
    pipeline: gst::Pipeline,
    sink: AppSink,
    sender: mpsc::Sender<Frame>,
    /// Loudest audio sample since the caller last reset it.
    pub peak: Arc<AtomicU16>,
    /// Source and settings that rebuild video in another codec.
    video: Option<(String, VideoSettings)>,
    _portal: Option<Portal>,
    audio: Option<ShareAudio>,
}

fn launch(description: &str) -> Result<gst::Pipeline, String> {
    gst::parse::launch(description)
        .map_err(|e| e.to_string())?
        .downcast::<gst::Pipeline>()
        .map_err(|_| "Invalid media pipeline".into())
}

fn audio_pipeline(device: &str) -> Result<gst::Pipeline, String> {
    let pipeline = launch(&format!(
        "pulsesrc name=input ! audioconvert ! volume name=audio_volume ! audioresample ! audio/x-raw,format=S16LE,rate=48000,channels=2 ! opusenc name=encoder bitrate={AUDIO_BITRATE} audio-type=generic frame-size=20 ! appsink name=encoded sync=false max-buffers=2 drop=true"
    ))?;
    let input = pipeline.by_name("input").ok_or("Missing audio source")?;
    input.set_property("device", (!device.is_empty()).then_some(device));
    Ok(pipeline)
}

/// GStreamer video from `source`, scaled on the GPU and encoded by NVENC.
fn video_pipeline(source: &str, settings: VideoSettings) -> Result<gst::Pipeline, String> {
    let VideoSettings {
        codec,
        width,
        height,
        fps,
        bitrate,
    } = settings.validate()?;
    gst::init().map_err(|e| e.to_string())?;
    let vbv = kbits(bitrate.min(VIDEO_START_BITRATE)) / 4;
    // P4 gains about 0.4 dB over P1, but H.265 P4 tops out near 4K60 on one shared NVENC engine.
    let quality_limit = if codec == Codec::H264 { 144 } else { 120 };
    let pixels = u64::from(width) * u64::from(height) * u64::from(fps);
    let preset = if pixels > 2560 * 1440 * quality_limit {
        "p1"
    } else {
        "p4"
    };
    let output = match codec {
        // Discord negotiates constrained-baseline H.264 for browser viewers.
        Codec::H264 => {
            "video/x-h264,stream-format=byte-stream,alignment=au,profile=constrained-baseline"
        }
        Codec::H265 => "video/x-h265,stream-format=byte-stream,alignment=au,profile=main",
        _ => "video/x-av1,stream-format=obu-stream,alignment=tu",
    };
    let headers = if codec == Codec::Av1 {
        ""
    } else {
        " repeat-sequence-header=true"
    };
    // Avoid periodic keyframes, whose bursts can overwhelm a limited send rate.
    // Bound the rate buffer to the startup budget to keep keyframes small after bandwidth falls.
    // Frames stay on the GPU from the DMA-BUF to NVENC. Rendering NV12 hands NVENC a PBO it can map.
    // The default GL shader scales without locking later size queries into passthrough.
    launch(&format!(
        "{source} ! videorate drop-only=true skip-to-first=true ! capsfilter name=video_rate caps=\"video/x-raw(ANY),framerate={fps}/1\" ! glupload ! glcolorconvert ! glshader ! video/x-raw(memory:GLMemory),width={width},height={height},pixel-aspect-ratio=1/1 ! glcolorconvert ! video/x-raw(memory:GLMemory),format=NV12 ! {} name=video_encoder preset={preset} tune=ultra-low-latency rc-mode=cbr bitrate={} vbv-buffer-size={vbv} spatial-aq=true gop-size=-1 bframes=0 rc-lookahead=0 zerolatency=true{headers} ! {output} ! appsink name=encoded sync=false max-buffers=2",
        codec.encoder(),
        kbits(bitrate),
    ))
}

impl Capture {
    /// The PulseAudio name of a device chosen by ID or by a unique display name.
    pub fn audio_device(selected: &str, outputs: bool) -> Result<String, String> {
        gst::init().map_err(|e| e.to_string())?;
        let provider = gst::DeviceProviderFactory::by_name("pulsedeviceprovider")
            .ok_or("Audio device provider unavailable")?;
        provider.start().map_err(|e| e.to_string())?;
        let class = if outputs {
            "Audio/Sink"
        } else {
            "Audio/Source"
        };
        let devices: Vec<_> = provider
            .devices()
            .iter()
            .filter(|device| device.device_class() == class)
            .map(|device| {
                let monitor = device
                    .properties()
                    .and_then(|p| p.get::<String>("device.class").ok())
                    .is_some_and(|class| class == "monitor");
                (
                    device.property::<String>("internal-name"),
                    device.display_name().to_string(),
                    monitor,
                )
            })
            .filter(|(id, ..)| !id.starts_with("relay-share-"))
            .collect();
        provider.stop();
        if let Some((id, ..)) = devices.iter().find(|(id, ..)| id == selected) {
            return Ok(id.clone());
        }
        let mut matches = devices
            .into_iter()
            .filter(|(_, name, monitor)| name == selected && !monitor);
        let (id, ..) = matches
            .next()
            .ok_or("Discord's selected audio device is unavailable")?;
        if matches.next().is_some() {
            return Err("Discord's selected audio device name is ambiguous".into());
        }
        Ok(id)
    }

    /// Encodes stereo without adding speech processing or changing system devices.
    /// An empty device selects the system default. `peak` tracks speaking activity.
    pub fn audio(device: &str) -> Result<(Self, mpsc::Receiver<Frame>), String> {
        gst::init().map_err(|e| e.to_string())?;
        let pipeline = audio_pipeline(device)?;
        let peak = Arc::new(AtomicU16::new(0));
        let level = peak.clone();
        pipeline
            .by_name("encoder")
            .and_then(|encoder| encoder.static_pad("sink"))
            .ok_or("Missing audio encoder")?
            .add_probe(gst::PadProbeType::BUFFER, move |_, info| {
                if let Some(map) = info.buffer().and_then(|b| b.map_readable().ok()) {
                    let samples = map.as_chunks::<2>().0.iter();
                    let loudest = samples.map(|s| i16::from_le_bytes(*s).unsigned_abs()).max();
                    level.fetch_max(loudest.unwrap_or(0), Ordering::Relaxed);
                }
                gst::PadProbeReturn::Ok
            });
        let (mut capture, frames) = Self::start(pipeline, Codec::Opus, None)?;
        capture.peak = peak;
        Ok((capture, frames))
    }

    pub fn set_audio_volume(&self, volume: f64) -> Result<(), String> {
        if !volume.is_finite() || !(0.0..=10.0).contains(&volume) {
            return Err("Invalid microphone volume".into());
        }
        let element = self
            .pipeline
            .by_name("audio_volume")
            .ok_or("Microphone volume missing")?;
        element.set_property("volume", volume);
        Ok(())
    }

    /// Captures other applications' audio, excluding call playback.
    pub async fn share_audio() -> Result<(Self, mpsc::Receiver<Frame>), String> {
        gst::init().map_err(|e| e.to_string())?;
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let name = format!(
            "relay-share-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let target = CString::new(name.clone()).map_err(|e| e.to_string())?;
        let router = tokio::task::spawn_blocking(move || {
            let mut router = std::ptr::null_mut();
            match unsafe { relay_share_audio_open(target.as_ptr(), &mut router) } {
                result if result < 0 => Err(format!(
                    "Could not isolate share audio: {}",
                    std::io::Error::from_raw_os_error(-result)
                )),
                _ => Ok(ShareAudio(router)),
            }
        })
        .await
        .map_err(|_| "Audio isolation stopped")??;
        let (mut capture, frames) = Self::start(
            audio_pipeline(&format!("{name}.monitor"))?,
            Codec::Opus,
            None,
        )?;
        capture.audio = Some(router);
        Ok((capture, frames))
    }

    /// Select one source through the portal. Nothing is saved for later capture.
    pub async fn screen(
        codec: Codec,
        quality: ScreenSettings,
    ) -> Result<(Self, mpsc::Receiver<Frame>, VideoSettings), String> {
        use ashpd::desktop::screencast::{
            CursorMode, Screencast, SelectSourcesOptions, SourceType,
        };
        use std::os::fd::AsRawFd;
        let quality = quality.validate()?;
        gst::init().map_err(|e| e.to_string())?;
        for name in [
            "pipewiresrc",
            codec.encoder(),
            "glupload",
            "glcolorconvert",
            "glshader",
        ] {
            gst::ElementFactory::find(name)
                .ok_or_else(|| format!("Missing native media plugin: {name}"))?;
        }
        let portal = Screencast::new().await.map_err(|e| e.to_string())?;
        // Finish creation so a cancelled request still closes its returned session.
        let (portal, mut owner) = tokio::spawn(async move {
            let session = portal
                .create_session(Default::default())
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>((
                portal,
                Portal {
                    session: Some(session),
                    _remote: None,
                },
            ))
        })
        .await
        .map_err(|_| "Screen session creation stopped")??;
        let session = owner.session.as_ref().unwrap();
        let options = SelectSourcesOptions::default()
            .set_cursor_mode(CursorMode::Embedded)
            .set_sources(SourceType::Monitor | SourceType::Window)
            .set_multiple(false)
            .set_persist_mode(ashpd::desktop::PersistMode::DoNot);
        let selected = portal.select_sources(session, options).await;
        selected
            .map_err(|e| e.to_string())?
            .response()
            .map_err(|e| e.to_string())?;
        let started = portal.start(session, None, Default::default()).await;
        let response = started
            .map_err(|e| e.to_string())?
            .response()
            .map_err(|e| e.to_string())?;
        let node = response
            .streams()
            .first()
            .ok_or("No screen selected")?
            .pipe_wire_node_id();
        let remote = portal
            .open_pipe_wire_remote(session, Default::default())
            .await
            .map_err(|e| e.to_string())?;
        // Keep the pipeline clock independent of the PipeWire stream rebuilt on codec changes.
        // FD and node are integers obtained from this portal, never pipeline text from the UI.
        let source = format!(
            "pipewiresrc fd={} path={node} provide-clock=false do-timestamp=true",
            remote.as_raw_fd()
        );
        let probe = format!(
            "{source} ! video/x-raw ! appsink name=size sync=false max-buffers=1 drop=true"
        );
        let (width, height, rate, remote) = tokio::task::spawn_blocking(move || {
            let pipeline = launch(&probe)?;
            let result = (|| {
                let sink = pipeline
                    .by_name("size")
                    .and_then(|s| s.downcast::<AppSink>().ok());
                pipeline
                    .set_state(gst::State::Playing)
                    .map_err(|e| e.to_string())?;
                let sample = sink
                    .and_then(|sink| sink.try_pull_sample(gst::ClockTime::from_seconds(5)))
                    .ok_or("Could not read capture dimensions")?;
                let caps = sample
                    .caps()
                    .and_then(|caps| caps.structure(0))
                    .ok_or("Missing capture dimensions")?;
                // Rounding up a fractional maximum can request an unsupported PipeWire format.
                let rate = ["framerate", "max-framerate"]
                    .into_iter()
                    .filter_map(|name| caps.get::<gst::Fraction>(name).ok())
                    .find(|rate| rate.numer() > 0)
                    .map_or(60, |rate| rate.numer() / rate.denom());
                let dimension = |name| {
                    caps.get::<i32>(name)
                        .map_err(|_| "Missing capture dimensions")
                };
                Ok::<_, String>((dimension("width")?, dimension("height")?, rate))
            })();
            let _ = pipeline.set_state(gst::State::Null);
            // Cancellation cannot close the FD while this blocking probe still uses it.
            result.map(|(width, height, rate)| (width, height, rate, remote))
        })
        .await
        .map_err(|_| "Screen size inspection stopped")??;
        owner._remote = Some(remote);
        let (width, height) = (f64::from(width.max(1)), f64::from(height.max(1)));
        let limit = if quality.height == 0 {
            2160
        } else {
            quality.height
        };
        let scale = (3840.0 / width).min(f64::from(limit) / height).min(1.0);
        let even = |size: f64| (size * scale) as u32 / 2 * 2;
        let (width, height) = (even(width), even(height));
        let fps = quality.fps.min(rate.clamp(1, 240) as u32);
        // H.265 and AV1 reach H.264's quality with about a third less bitrate.
        let bits_per_pixel = if codec == Codec::H264 { 0.15 } else { 0.1 };
        let bitrate = (f64::from(width * height * fps) * bits_per_pixel) as u32 / 1000 * 1000;
        let bitrate = bitrate.clamp(4_000_000, 50_000_000);
        let settings = VideoSettings {
            codec,
            width,
            height,
            fps,
            bitrate,
        }
        .validate()?;
        // The source paces frames at their damage times, up to the chosen rate, without duplicates.
        // The last frame repeats after a quiet quarter second so keyframe requests on a static screen still complete.
        let source =
            format!("{source} keepalive-time=250 ! video/x-raw(ANY),max-framerate={fps}/1");
        let initial = VideoSettings {
            bitrate: bitrate.min(VIDEO_START_BITRATE),
            ..settings
        };
        let pipeline = video_pipeline(&source, initial)?;
        let (mut capture, frames) = Self::start(pipeline, codec, Some((source, initial)))?;
        capture._portal = Some(owner);
        Ok((capture, frames, settings))
    }

    fn start(
        pipeline: gst::Pipeline,
        codec: Codec,
        video: Option<(String, VideoSettings)>,
    ) -> Result<(Self, mpsc::Receiver<Frame>), String> {
        let (sender, receiver) = mpsc::channel(4);
        let mut capture = Self {
            sink: Self::sink(&pipeline)?,
            pipeline,
            sender,
            peak: Arc::default(),
            video,
            _portal: None,
            audio: None,
        };
        capture.listen(codec)?;
        Ok((capture, receiver))
    }

    fn sink(pipeline: &gst::Pipeline) -> Result<AppSink, String> {
        let sink = pipeline.by_name("encoded").ok_or("Missing encoded sink")?;
        sink.downcast::<AppSink>()
            .map_err(|_| "Invalid encoded sink".into())
    }

    fn listen(&mut self, codec: Codec) -> Result<(), String> {
        let sender = self.sender.clone();
        let pending = Arc::new(Mutex::new(VecDeque::<mpsc::OwnedPermit<Frame>>::new()));
        if codec != Codec::Opus {
            let (pending, sender) = (pending.clone(), sender.clone());
            self.pipeline
                .by_name("video_rate")
                .and_then(|rate| rate.static_pad("src"))
                .ok_or("Missing video rate pad")?
                .add_probe(gst::PadProbeType::BUFFER, move |_, _| {
                    // Reserve before encoding. Losing an encoded frame would break later references.
                    match sender.clone().try_reserve_owned() {
                        Ok(permit) => {
                            pending.lock().unwrap().push_back(permit);
                            gst::PadProbeReturn::Ok
                        }
                        Err(_) => gst::PadProbeReturn::Drop,
                    }
                });
        }
        let callbacks = AppSinkCallbacks::builder().new_sample(move |sink| {
            let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
            let buffer = sample.buffer().ok_or(gst::FlowError::Error)?;
            let permit = if codec != Codec::Opus {
                pending
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or(gst::FlowError::Error)?
            } else {
                match sender.clone().try_reserve_owned() {
                    Ok(permit) => permit,
                    Err(mpsc::error::TrySendError::Full(_)) => return Ok(gst::FlowSuccess::Ok),
                    Err(mpsc::error::TrySendError::Closed(_)) => return Err(gst::FlowError::Eos),
                }
            };
            // Encoders offset PTS (often by 1000 hours). The segment maps it
            // back to the shared running time, including after a codec switch.
            let pts = sample
                .segment()
                .and_then(|segment| segment.downcast_ref::<gst::ClockTime>())
                .and_then(|segment| segment.to_running_time(buffer.pts()?))
                .ok_or(gst::FlowError::Error)?;
            let keyframe = !buffer.flags().contains(gst::BufferFlags::DELTA_UNIT);
            let data = buffer
                .to_owned()
                .into_mapped_buffer_readable()
                .map_err(|_| gst::FlowError::Error)?;
            permit.send(Frame {
                codec,
                pts,
                data,
                keyframe,
            });
            Ok(gst::FlowSuccess::Ok)
        });
        self.sink.set_callbacks(callbacks.build());
        self.pipeline.set_state(gst::State::Playing).map_err(|_| {
            self.error()
                .unwrap_or_else(|| "Could not start native capture".into())
        })?;
        Ok(())
    }

    /// Rebuilds video in another codec from the same portal stream, without a new prompt.
    /// A new encoder starts with a keyframe at the current bitrate.
    pub fn set_codec(&mut self, codec: Codec) -> Result<(), String> {
        let (source, settings) = self.video.clone().ok_or("Capture has no video")?;
        if settings.codec == codec {
            return Ok(());
        }
        let settings = VideoSettings { codec, ..settings };
        let pipeline = video_pipeline(&source, settings)?;
        let sink = Self::sink(&pipeline)?;
        // RTP keeps its timeline across codec changes. Do not restart source timestamps.
        pipeline.use_clock(self.pipeline.clock().as_ref());
        pipeline.set_start_time(gst::ClockTime::NONE);
        pipeline.set_base_time(self.pipeline.base_time().ok_or("Capture clock missing")?);
        // Stop the old source before reconnecting capture.
        let _ = self.pipeline.set_state(gst::State::Null);
        self.sink.set_callbacks(AppSinkCallbacks::builder().build());
        self.pipeline = pipeline;
        self.sink = sink;
        self.video = Some((source, settings));
        self.listen(codec)
    }

    pub fn error(&self) -> Option<String> {
        if let Some(audio) = &self.audio {
            let error = unsafe { relay_share_audio_error(audio.0) };
            if error != 0 {
                return Some(std::io::Error::from_raw_os_error(error).to_string());
            }
        }
        let message = self
            .pipeline
            .bus()?
            .pop_filtered(&[gst::MessageType::Error, gst::MessageType::Eos])?;
        Some(match message.view() {
            gst::MessageView::Error(error) => error.error().to_string(),
            _ => "Capture ended".into(),
        })
    }

    /// Updates rate control without resetting the encoder's reference chain.
    pub fn set_video_bitrate(&mut self, bitrate: u32) -> Result<(), String> {
        let (_, settings) = self.video.as_mut().ok_or("Capture has no video")?;
        let updated = VideoSettings {
            bitrate,
            ..*settings
        }
        .validate()
        .map_err(|_| "Invalid video bitrate")?;
        let encoder = self
            .pipeline
            .by_name("video_encoder")
            .ok_or("Video encoder missing")?;
        encoder.set_property("bitrate", kbits(bitrate));
        *settings = updated;
        Ok(())
    }

    pub fn request_keyframe(&self) -> Result<(), String> {
        let event = gstreamer_video::UpstreamForceKeyUnitEvent::builder()
            .all_headers(true)
            .build();
        self.sink
            .send_event(event)
            .then_some(())
            .ok_or_else(|| "Encoder rejected the keyframe request".into())
    }
}

impl Drop for Capture {
    fn drop(&mut self) {
        // NULL waits for streaming tasks before releasing the callback's channel sender.
        let _ = self.pipeline.set_state(gst::State::Null);
        self.sink.set_callbacks(AppSinkCallbacks::builder().build());
    }
}
