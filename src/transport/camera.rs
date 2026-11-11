use gstreamer::{self as gst, prelude::*};
use gstreamer_app::{AppSink, AppSrc};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::mpsc;

pub(super) struct Camera {
    pub user: String,
    pub rtx: u32,
    pub seen: tokio::time::Instant,
    pub recovery: Arc<AtomicU64>,
    ssrc: u32,
    pipeline: gst::Pipeline,
    source: AppSrc,
}

pub(crate) struct Picture {
    pub ssrc: u32,
    pub data: Vec<u8>,
    pub timestamp: u64,
    pub keyframe: bool,
    recovery: Arc<AtomicU64>,
    generation: u64,
}

impl Picture {
    pub fn valid(&self) -> bool {
        self.keyframe || self.recovery.load(Ordering::Relaxed) & 1 == 0
    }
    pub fn lost(&self) {
        let _ = self
            .recovery
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.wrapping_add(2) | 1)
            });
    }
    pub fn delivered(&self) {
        if self.keyframe {
            // A later overflow must not be cleared by an older queued keyframe.
            let _ = self.recovery.compare_exchange(
                self.generation,
                self.generation & !1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }
}

impl Camera {
    pub fn new(
        user: String,
        ssrc: u32,
        rtx: u32,
        frames: mpsc::Sender<Picture>,
    ) -> Result<Self, String> {
        gst::init().map_err(|e| e.to_string())?;
        let pipeline = gst::parse::launch(
            "appsrc name=input is-live=true format=time do-timestamp=true max-buffers=256 leaky-type=downstream caps=application/x-rtp,media=video,encoding-name=H264,clock-rate=90000,payload=101 ! rtpjitterbuffer latency=100 drop-on-latency=false do-lost=true ! rtph264depay wait-for-keyframe=true ! video/x-h264,stream-format=byte-stream,alignment=au ! appsink name=output sync=false max-buffers=4 drop=true",
        ).map_err(|e| e.to_string())?.downcast::<gst::Pipeline>().map_err(|_| "Invalid camera pipeline")?;
        let source = pipeline
            .by_name("input")
            .unwrap()
            .downcast::<AppSrc>()
            .unwrap();
        let sink = pipeline
            .by_name("output")
            .unwrap()
            .downcast::<AppSink>()
            .unwrap();
        let recovery = Arc::new(AtomicU64::new(1));
        let waiting = recovery.clone();
        sink.set_callbacks(
            gstreamer_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gst::FlowError::Error)?;
                    let data = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                    let keyframe = !buffer.flags().contains(gst::BufferFlags::DELTA_UNIT);
                    let generation = waiting.load(Ordering::Relaxed);
                    if data.len() <= 1024 * 1024 {
                        let picture = Picture {
                            ssrc,
                            data: data.to_vec(),
                            timestamp: buffer.pts().unwrap_or(gst::ClockTime::ZERO).nseconds(),
                            keyframe,
                            generation,
                            recovery: waiting.clone(),
                        };
                        if let Err(error) = frames.try_send(picture) {
                            error.into_inner().lost();
                        }
                    } else {
                        let _ =
                            waiting.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                                Some(value.wrapping_add(2) | 1)
                            });
                    }
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
        pipeline
            .set_state(gst::State::Playing)
            .map_err(|e| e.to_string())?;
        Ok(Self {
            user,
            rtx,
            recovery,
            ssrc,
            pipeline,
            source,
            seen: tokio::time::Instant::now() - std::time::Duration::from_secs(2),
        })
    }

    pub fn push(&self, packet: super::rtp::Packet<'_>, repair: bool) {
        let (sequence, payload) = if repair {
            let Some(bytes) = packet.payload.get(..2) else {
                return;
            };
            (
                u16::from_be_bytes([bytes[0], bytes[1]]),
                &packet.payload[2..],
            )
        } else {
            (packet.sequence, packet.payload)
        };
        let mut data = Vec::with_capacity(12 + payload.len());
        data.extend_from_slice(&[0x80, 101 | (u8::from(packet.marker) << 7)]);
        data.extend_from_slice(&sequence.to_be_bytes());
        data.extend_from_slice(&packet.timestamp.to_be_bytes());
        // Repairs are fed into the same jitter buffer as original packets.
        data.extend_from_slice(&self.ssrc.to_be_bytes());
        data.extend_from_slice(payload);
        let _ = self.source.push_buffer(gst::Buffer::from_mut_slice(data));
    }
}

impl Drop for Camera {
    fn drop(&mut self) {
        let _ = self.pipeline.set_state(gst::State::Null);
    }
}
