use crate::{capture::Capture, transport::ReceivedFrame};
use gstreamer::{self as gst, prelude::*};
use gstreamer_app::AppSrc;
use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

struct Speaker {
    bin: gst::Bin,
    input: AppSrc,
    pad: gst::Pad,
    seen: Instant,
    voiced: Option<Instant>,
}

/// RTP timing, concealment and mixing run on GStreamer's streaming threads.
pub struct Playback {
    pipeline: gst::Pipeline,
    mixer: gst::Element,
    speakers: HashMap<u64, Speaker>,
    volumes: HashMap<u64, f64>,
    first: Option<Speaker>,
}

impl Playback {
    pub fn new(device: &str) -> Result<Self, String> {
        gst::init().map_err(|e| e.to_string())?;
        #[cfg(target_os = "linux")]
        let sink = "pulsesink buffer-time=30000 latency-time=10000 stream-properties=props,node.name=relay-playback,application.name=Relay";
        #[cfg(target_os = "windows")]
        let sink = "wasapi2sink low-latency=true";
        // Give loss notifications and decoding one packet's processing time before mixing.
        let pipeline = gst::parse::launch(&format!(
            "audiomixer name=mix force-live=true ignore-inactive-pads=true min-upstream-latency=40000000 latency=20000000 ! audioconvert ! audioresample ! audio/x-raw,rate=48000,channels=2 ! volume name=volume ! {sink} name=output"
        )).map_err(|e| e.to_string())?.downcast::<gst::Pipeline>().map_err(|_| "Invalid playback pipeline")?;
        let mixer = pipeline.by_name("mix").unwrap();
        let mut playback = Self {
            pipeline,
            mixer,
            speakers: HashMap::new(),
            volumes: HashMap::new(),
            first: None,
        };
        // Load and link the first decoder before RTP starts arriving.
        playback.first = Some(playback.create_speaker()?);
        playback.set_device(device)?;
        Ok(playback)
    }

    pub fn set_device(&self, selected: &str) -> Result<(), String> {
        let device = if selected.is_empty() {
            None
        } else {
            Some(Capture::audio_device(selected, true)?)
        };
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

    pub fn set_volume(&mut self, volume: f64, users: &HashMap<u64, f64>) {
        self.pipeline
            .by_name("volume")
            .unwrap()
            .set_property("volume", volume.clamp(0.0, 10.0));
        for (user, speaker) in &self.speakers {
            speaker.pad.set_property(
                "volume",
                users.get(user).copied().unwrap_or(1.0).clamp(0.0, 10.0),
            );
        }
        self.volumes.clone_from(users);
    }

    pub fn push(&mut self, frame: ReceivedFrame) -> Result<(), String> {
        if !self.speakers.contains_key(&frame.user) {
            if self.speakers.len() >= 128 {
                return Err("Too many audio participants".into());
            }
            let speaker = match self.first.take() {
                Some(speaker) => speaker,
                None => self.create_speaker()?,
            };
            speaker.pad.set_property(
                "volume",
                self.volumes
                    .get(&frame.user)
                    .copied()
                    .unwrap_or(1.0)
                    .clamp(0.0, 10.0),
            );
            self.speakers.insert(frame.user, speaker);
        }
        let speaker = self.speakers.get_mut(&frame.user).unwrap();
        speaker.seen = Instant::now();
        if frame.data.len() > 24 {
            speaker.voiced = Some(speaker.seen);
        }
        let mut packet = Vec::with_capacity(12 + frame.data.len());
        packet.extend_from_slice(&[0x80, 120 | (u8::from(frame.marker) << 7)]);
        packet.extend_from_slice(&frame.sequence.to_be_bytes());
        packet.extend_from_slice(&frame.timestamp.to_be_bytes());
        packet.extend_from_slice(&frame.ssrc.to_be_bytes());
        packet.extend(frame.data);
        speaker
            .input
            .push_buffer(gst::Buffer::from_mut_slice(packet))
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    fn create_speaker(&self) -> Result<Speaker, String> {
        let bin = gst::parse::bin_from_description(
            "appsrc name=input is-live=true do-timestamp=true format=time max-buffers=32 leaky-type=downstream caps=application/x-rtp,media=audio,encoding-name=OPUS,clock-rate=48000,payload=120 ! rtpjitterbuffer latency=40 drop-on-latency=true do-lost=true ! rtpopusdepay ! opusdec plc=true ! audioconvert", true
        ).map_err(|e| e.to_string())?;
        let input = bin.by_name("input").unwrap().downcast::<AppSrc>().unwrap();
        self.pipeline.add(&bin).map_err(|e| e.to_string())?;
        bin.link(&self.mixer).map_err(|e| e.to_string())?;
        let pad = bin.static_pad("src").unwrap().peer().unwrap();
        bin.sync_state_with_parent().map_err(|e| e.to_string())?;
        Ok(Speaker {
            bin,
            input,
            pad,
            seen: Instant::now(),
            voiced: None,
        })
    }

    pub fn speakers(&mut self) -> Vec<u64> {
        let mut active = Vec::new();
        self.speakers.retain(|&user, speaker| {
            if speaker.seen.elapsed() > Duration::from_secs(30) {
                let _ = speaker.bin.set_state(gst::State::Null);
                speaker.bin.unlink(&self.mixer);
                self.mixer.release_request_pad(&speaker.pad);
                let _ = self.pipeline.remove(&speaker.bin);
                return false;
            }
            if speaker
                .voiced
                .is_some_and(|t| t.elapsed() < Duration::from_millis(250))
            {
                active.push(user);
            }
            true
        });
        active
    }

    pub fn error(&self) -> Option<String> {
        for message in self
            .pipeline
            .bus()?
            .iter_filtered(&[gst::MessageType::Error, gst::MessageType::Latency])
        {
            match message.view() {
                gst::MessageView::Error(error) => return Some(error.error().to_string()),
                gst::MessageView::Latency(_) => {
                    if let Err(error) = self.pipeline.recalculate_latency() {
                        return Some(error.to_string());
                    }
                }
                _ => (),
            }
        }
        None
    }
}

impl Drop for Playback {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}
