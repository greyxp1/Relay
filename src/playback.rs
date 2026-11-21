use crate::{capture::Capture, transport::ReceivedFrame};
use gstreamer::{self as gst, prelude::*};
use gstreamer_app::AppSrc;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};
use tokio::task::{spawn_blocking, JoinHandle};
use tokio_tungstenite::tungstenite::Bytes;

const STREAM_FRAMES: u64 = 512;
pub const STREAM_AUDIO_BYTES: usize = STREAM_FRAMES as usize * 2 * size_of::<f32>();

struct Speaker {
    bin: gst::Bin,
    input: AppSrc,
    pad: gst::Pad,
    ssrc: Option<u32>,
    seen: Instant,
    voiced: Option<Instant>,
}

#[derive(Default)]
enum StreamPlayback {
    #[default]
    Idle,
    Starting(JoinHandle<Result<StreamOutput, String>>),
    Playing(StreamOutput),
    Failed,
}

struct StreamOutput {
    pipeline: gst::Pipeline,
    input: AppSrc,
}

impl StreamOutput {
    fn new(device: Option<&str>) -> Result<Self, String> {
        let pipeline = gst::parse::launch(
            "appsrc name=input is-live=true do-timestamp=true format=time max-buffers=3 leaky-type=downstream caps=\"audio/x-raw,format=F32LE,layout=interleaved,rate=48000,channels=2\" ! pulsesink name=output async=false sync=false buffer-time=50000 latency-time=10000 stream-properties=props,node.name=relay-stream-playback,application.name=Relay",
        )
        .map_err(|e| e.to_string())?
        .downcast::<gst::Pipeline>()
        .map_err(|_| "Invalid stream audio pipeline")?;
        let stream = Self {
            input: pipeline
                .by_name("input")
                .unwrap()
                .downcast::<AppSrc>()
                .unwrap(),
            pipeline,
        };
        stream
            .pipeline
            .by_name("output")
            .unwrap()
            .set_property("device", device);
        stream
            .pipeline
            .set_state(gst::State::Playing)
            .map_err(|e| e.to_string())?;
        Ok(stream)
    }
}

impl Drop for StreamOutput {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

/// RTP timing, concealment and mixing run on GStreamer's streaming threads.
pub struct Playback {
    pipeline: gst::Pipeline,
    mixer: gst::Element,
    speakers: HashMap<u64, Speaker>,
    volumes: HashMap<u64, f64>,
    first: Option<Speaker>,
    stream: StreamPlayback,
}

impl Playback {
    pub fn new(device: &str) -> Result<Self, String> {
        gst::init().map_err(|e| e.to_string())?;
        // Give loss notifications and decoding one packet's processing time before mixing.
        let pipeline = gst::parse::launch(
            "audiomixer name=mix force-live=true ignore-inactive-pads=true min-upstream-latency=60000000 latency=20000000 ! audio/x-raw,format=F32LE,rate=48000,channels=2 ! volume name=volume ! audioconvert ! audioresample ! pulsesink name=output buffer-time=60000 latency-time=20000 stream-properties=props,node.name=relay-playback,application.name=Relay",
        )
        .map_err(|e| e.to_string())?
        .downcast::<gst::Pipeline>()
        .map_err(|_| "Invalid playback pipeline")?;
        let mixer = pipeline.by_name("mix").unwrap();
        let mut playback = Self {
            pipeline,
            mixer,
            speakers: HashMap::new(),
            volumes: HashMap::new(),
            first: None,
            stream: StreamPlayback::default(),
        };
        // Load and link the first decoder before RTP starts arriving.
        playback.first = Some(playback.create_speaker()?);
        playback.set_device(device)?;
        Ok(playback)
    }

    pub fn set_device(&mut self, selected: &str) -> Result<(), String> {
        let device = (!selected.is_empty())
            .then(|| Capture::audio_device(selected, true))
            .transpose()?;
        self.pipeline
            .set_state(gst::State::Null)
            .map_err(|e| e.to_string())?;
        let output = self.pipeline.by_name("output").unwrap();
        let previous = output.property::<Option<String>>("device");
        output.set_property("device", device.as_deref());
        if let Err(error) = self.start() {
            let _ = self.pipeline.set_state(gst::State::Null);
            if let Some(bus) = self.pipeline.bus() {
                for _ in bus.iter_filtered(&[gst::MessageType::Error]) {}
            }
            output.set_property("device", previous.as_deref());
            self.start().map_err(|restore| {
                format!("{error}. Restoring the previous speaker failed: {restore}")
            })?;
            return Err(error);
        }
        self.stream = StreamPlayback::Idle;
        Ok(())
    }

    fn start(&self) -> Result<(), String> {
        self.pipeline
            .set_state(gst::State::Playing)
            .map_err(|e| e.to_string())?;
        let (result, state, _) = self.pipeline.state(gst::ClockTime::from_seconds(2));
        result.map_err(|e| e.to_string())?;
        if state != gst::State::Playing {
            return Err("Speaker did not start".into());
        }
        Ok(())
    }

    fn user_volume(&self, user: u64) -> f64 {
        self.volumes
            .get(&user)
            .copied()
            .unwrap_or(1.0)
            .clamp(0.0, 10.0)
    }

    pub fn set_volume(&mut self, volume: f64, users: &HashMap<u64, f64>) {
        self.pipeline
            .by_name("volume")
            .unwrap()
            .set_property("volume", volume.clamp(0.0, 10.0));
        self.volumes.clone_from(users);
        for (user, speaker) in &self.speakers {
            speaker.pad.set_property("volume", self.user_volume(*user));
        }
    }

    fn remove_speaker(&self, speaker: Speaker) {
        let _ = speaker.bin.set_state(gst::State::Null);
        speaker.bin.unlink(&self.mixer);
        self.mixer.release_request_pad(&speaker.pad);
        let _ = self.pipeline.remove(&speaker.bin);
    }

    pub fn push(&mut self, frame: ReceivedFrame) -> Result<(), String> {
        let ReceivedFrame { user, ssrc, packet } = frame;
        if self
            .speakers
            .get(&user)
            .is_some_and(|s| s.ssrc.is_some_and(|old| old != ssrc))
        {
            let speaker = self.speakers.remove(&user).unwrap();
            self.remove_speaker(speaker);
        }
        if !self.speakers.contains_key(&user) {
            if self.speakers.len() >= 128 {
                return Err("Too many audio participants".into());
            }
            let speaker = match self.first.take() {
                Some(speaker) => speaker,
                None => self.create_speaker()?,
            };
            speaker.pad.set_property("volume", self.user_volume(user));
            self.speakers.insert(user, speaker);
        }
        let speaker = self.speakers.get_mut(&user).unwrap();
        speaker.ssrc = Some(ssrc);
        speaker.seen = Instant::now();
        // Opus DTX comfort-noise frames are a few bytes. Anything larger carries voice.
        if packet.len() > 12 + 24 {
            speaker.voiced = Some(speaker.seen);
        }
        speaker
            .input
            .push_buffer(gst::Buffer::from_mut_slice(packet))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    pub async fn push_stream(&mut self, samples: Bytes) -> Result<(), String> {
        match &mut self.stream {
            StreamPlayback::Idle => {
                let device = self
                    .pipeline
                    .by_name("output")
                    .unwrap()
                    .property::<Option<String>>("device");
                self.stream = StreamPlayback::Starting(spawn_blocking(move || {
                    StreamOutput::new(device.as_deref())
                }));
            }
            StreamPlayback::Starting(start) if start.is_finished() => {
                let result = start
                    .await
                    .map_err(|_| "Stream audio setup stopped".to_owned())
                    .and_then(|result| result);
                self.stream = StreamPlayback::Failed;
                self.stream = StreamPlayback::Playing(result?);
            }
            _ => {}
        }
        let StreamPlayback::Playing(stream) = &self.stream else {
            return Ok(());
        };
        let mut buffer = gst::Buffer::from_slice(samples);
        buffer
            .get_mut()
            .unwrap()
            .set_duration(gst::ClockTime::from_nseconds(
                STREAM_FRAMES * 1_000_000_000 / 48_000,
            ));
        if let Err(error) = stream.input.push_buffer(buffer) {
            self.stream = StreamPlayback::Failed;
            return Err(error.to_string());
        }
        Ok(())
    }

    pub fn stream_error(&mut self) -> Option<String> {
        let StreamPlayback::Playing(stream) = &self.stream else {
            return None;
        };
        let error = pipeline_error(&stream.pipeline)?;
        self.stream = StreamPlayback::Failed;
        Some(error)
    }

    fn create_speaker(&self) -> Result<Speaker, String> {
        // Keep queued loss notifications so the Opus decoder can conceal missing packets.
        let bin = gst::parse::bin_from_description(
            "appsrc name=input is-live=true do-timestamp=true format=time max-buffers=32 leaky-type=downstream caps=application/x-rtp,media=audio,encoding-name=OPUS,clock-rate=48000,payload=120 ! rtpjitterbuffer latency=60 do-lost=true max-dropout-time=1000 max-misorder-time=500 ! rtpopusdepay ! opusdec plc=true use-inband-fec=true ! audioconvert ! capsfilter caps=\"audio/x-raw,format=F32LE,rate=48000,channels=2\"",
            true,
        )
        .map_err(|e| e.to_string())?;
        let input = bin.by_name("input").unwrap().downcast::<AppSrc>().unwrap();
        self.pipeline.add(&bin).map_err(|e| e.to_string())?;
        bin.link(&self.mixer).map_err(|e| e.to_string())?;
        let pad = bin.static_pad("src").unwrap().peer().unwrap();
        bin.sync_state_with_parent().map_err(|e| e.to_string())?;
        let _ = self.pipeline.recalculate_latency();
        Ok(Speaker {
            bin,
            input,
            pad,
            ssrc: None,
            seen: Instant::now(),
            voiced: None,
        })
    }

    pub fn speakers(&mut self) -> Vec<u64> {
        let expired: Vec<_> = self
            .speakers
            .iter()
            .filter(|(_, s)| s.seen.elapsed() > Duration::from_secs(30))
            .map(|(user, _)| *user)
            .collect();
        for user in expired {
            let speaker = self.speakers.remove(&user).unwrap();
            self.remove_speaker(speaker);
        }
        let voiced = |s: &Speaker| {
            s.voiced
                .is_some_and(|t| t.elapsed() < Duration::from_millis(250))
        };
        self.speakers
            .iter()
            .filter(|(_, s)| voiced(s))
            .map(|(user, _)| *user)
            .collect()
    }

    pub fn error(&self) -> Option<String> {
        pipeline_error(&self.pipeline)
    }
}

fn pipeline_error(pipeline: &gst::Pipeline) -> Option<String> {
    for message in pipeline
        .bus()?
        .iter_filtered(&[gst::MessageType::Error, gst::MessageType::Latency])
    {
        match message.view() {
            gst::MessageView::Error(error) => return Some(error.error().to_string()),
            _ => {
                if let Err(error) = pipeline.recalculate_latency() {
                    return Some(error.to_string());
                }
            }
        }
    }
    None
}

impl Drop for Playback {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn speaker_bin_parses() {
        let playback = super::Playback::new("").unwrap();
        let speaker = playback.create_speaker().unwrap();
        playback.remove_speaker(speaker);
    }
}
