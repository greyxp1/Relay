//! Token-bucket video pacing with a bounded RTX history.
use std::{
    collections::VecDeque,
    ops::Range,
    rc::Rc,
    time::{Duration, Instant},
};

const WIRE_OVERHEAD: usize = 80;
pub(super) const MAX_PAYLOAD: usize = super::rtp::MTU - 54;
const MAX_QUEUE_BYTES: usize = 2 * 1024 * 1024;
const QUEUE_AGE: Duration = Duration::from_millis(250);
const HISTORY_AGE: Duration = Duration::from_millis(500);
const REPAIR_INTERVAL: Duration = Duration::from_millis(40);
const REPAIR_SHARE: f64 = 0.2;

/// RTP payloads of one frame, stored back to back to avoid an allocation per packet.
#[derive(Default)]
pub(super) struct Payloads {
    data: Vec<u8>,
    ends: Vec<usize>,
}

impl Payloads {
    pub fn push(&mut self, parts: &[&[u8]]) {
        for part in parts {
            self.data.extend_from_slice(part);
        }
        self.ends.push(self.data.len());
    }

    /// Bytes of the payload being built.
    pub fn open(&mut self) -> &mut Vec<u8> {
        &mut self.data
    }

    pub fn close(&mut self) {
        self.ends.push(self.data.len());
    }
}

pub(super) struct Packet {
    pub timestamp: u32,
    pub marker: bool,
    pub original_sequence: Option<u16>,
    data: Rc<Vec<u8>>,
    range: Range<usize>,
}

impl Packet {
    pub fn payload(&self) -> &[u8] {
        &self.data[self.range.clone()]
    }
}

struct Frame {
    timestamp: u32,
    deadline: Instant,
    keyframe: bool,
    data: Rc<Vec<u8>>,
    ends: Vec<usize>,
    next: usize,
}

impl Frame {
    fn range(&self) -> Range<usize> {
        self.next.checked_sub(1).map_or(0, |i| self.ends[i])..self.ends[self.next]
    }
}

struct History {
    sequence: u16,
    packet: Packet,
    sent: Instant,
    repairs: u8,
    nacked: bool,
    last_repair: Option<Instant>,
}

struct Budget {
    rate: f64,
    capacity: f64,
    tokens: f64,
    updated: Instant,
}

impl Budget {
    fn new(rate: f64, now: Instant) -> Self {
        let capacity = (rate * 0.010).max(2.0 * (MAX_PAYLOAD + WIRE_OVERHEAD + 2) as f64);
        Self {
            rate,
            capacity,
            tokens: capacity,
            updated: now,
        }
    }

    fn available(&self, now: Instant) -> f64 {
        let earned = now.saturating_duration_since(self.updated).as_secs_f64() * self.rate;
        (self.tokens + earned).min(self.capacity)
    }

    fn ready(&self, bytes: usize, now: Instant) -> Instant {
        let missing = (bytes as f64 - self.available(now)).max(0.0);
        now + Duration::from_secs_f64(missing / self.rate)
    }

    fn spend(&mut self, bytes: usize, now: Instant) {
        self.tokens = self.available(now) - bytes as f64;
        self.updated = now;
    }

    fn set_rate(&mut self, rate: f64, now: Instant) {
        let available = self.available(now);
        *self = Self::new(rate, now);
        self.tokens = available.min(self.capacity);
    }
}

/// Time to send `bytes` of queued media while leaving room for repairs.
fn drain_time(bytes: usize, rate: f64) -> Duration {
    Duration::from_secs_f64(bytes as f64 / (rate * (1.0 - REPAIR_SHARE)))
}

/// Wire bytes per second for an encoder bitrate, with headroom for overhead.
fn wire_rate(bitrate: u32) -> f64 {
    f64::from(bitrate.max(1)) / 8.0 * 1.25
}

pub(super) struct Pacer {
    frames: VecDeque<Frame>,
    queued_bytes: usize,
    queued_packets: usize,
    history: VecDeque<History>,
    history_bytes: usize,
    repairs: VecDeque<u16>,
    wire: Budget,
    repair: Budget,
    last_keyframe: Option<Instant>,
}

impl Pacer {
    pub fn new(bitrate: u32, now: Instant) -> Self {
        let rate = wire_rate(bitrate);
        Self {
            frames: VecDeque::new(),
            queued_bytes: 0,
            queued_packets: 0,
            history: VecDeque::new(),
            history_bytes: 0,
            repairs: VecDeque::new(),
            wire: Budget::new(rate, now),
            repair: Budget::new(rate * REPAIR_SHARE, now),
            last_keyframe: None,
        }
    }

    pub fn set_bitrate(&mut self, bitrate: u32, now: Instant) {
        let rate = wire_rate(bitrate);
        if rate < self.wire.rate {
            let mut bytes = 0;
            for frame in &mut self.frames {
                bytes += frame.data.len() + frame.ends.len() * WIRE_OVERHEAD;
                frame.deadline = frame
                    .deadline
                    .max(now + QUEUE_AGE + drain_time(bytes, rate));
            }
        }
        self.wire.set_rate(rate, now);
        self.repair.set_rate(rate * REPAIR_SHARE, now);
    }

    fn clear_frames(&mut self) {
        self.frames.clear();
        self.queued_bytes = 0;
        self.queued_packets = 0;
    }

    pub fn enqueue(
        &mut self,
        timestamp: u32,
        keyframe: bool,
        payloads: Payloads,
        now: Instant,
    ) -> bool {
        if self.expire(now) {
            return false;
        }
        let Payloads { data, ends } = payloads;
        let mut start = 0;
        let valid = ends.iter().all(|&end| {
            let size = end - std::mem::replace(&mut start, end);
            (1..=MAX_PAYLOAD).contains(&size)
        });
        if !valid
            || ends.is_empty()
            || data.len() > 1024 * 1024
            || self.queued_bytes + data.len() > MAX_QUEUE_BYTES
            || self.queued_packets + ends.len() > 2048
        {
            self.clear_frames();
            return false;
        }
        self.queued_bytes += data.len();
        self.queued_packets += ends.len();
        // Include transmission time even for a large keyframe, with room for repairs.
        let queued = self.queued_bytes + self.queued_packets * WIRE_OVERHEAD;
        let deadline = now + QUEUE_AGE + drain_time(queued, self.wire.rate);
        let data = Rc::new(data);
        self.frames.push_back(Frame {
            timestamp,
            deadline,
            keyframe,
            data,
            ends,
            next: 0,
        });
        true
    }

    pub fn has_keyframe(&self, now: Instant) -> bool {
        self.frames.iter().any(|frame| frame.keyframe)
            || self
                .last_keyframe
                .is_some_and(|sent| now.saturating_duration_since(sent) < HISTORY_AGE)
    }

    pub fn can_enqueue(&self) -> bool {
        // Keep at most 50 ms plus one frame queued, applying pressure before encoding.
        ((self.queued_bytes + self.queued_packets * WIRE_OVERHEAD) as f64) < self.wire.rate * 0.05
    }

    fn trim_history(&mut self, now: Instant) {
        while self.history.front().is_some_and(|entry| {
            now.saturating_duration_since(entry.sent) >= HISTORY_AGE
                || self.history.len() > 1024
                || self.history_bytes > MAX_QUEUE_BYTES
        }) {
            self.history_bytes -= self.history.pop_front().unwrap().packet.range.len();
        }
        let first = self.history.front().map(|entry| entry.sequence);
        let len = self.history.len();
        self.repairs.retain(|sequence| {
            first.is_some_and(|first| usize::from(sequence.wrapping_sub(first)) < len)
        });
    }

    /// Drops queued frames that missed their deadline and reports whether any did.
    pub fn expire(&mut self, now: Instant) -> bool {
        self.trim_history(now);
        let expired = self
            .frames
            .front()
            .is_some_and(|frame| now >= frame.deadline);
        if expired {
            self.clear_frames();
        }
        expired
    }

    fn history_index(&self, sequence: u16) -> Option<usize> {
        // Primary sequences are contiguous, including wraparound. Repairs use their own RTP sender.
        let index = usize::from(sequence.wrapping_sub(self.history.front()?.sequence));
        (index < self.history.len()).then_some(index)
    }

    fn repair_entry(&self, sequence: u16, now: Instant) -> Option<&History> {
        self.history
            .get(self.history_index(sequence)?)
            .filter(|entry| {
                now.saturating_duration_since(entry.sent) < HISTORY_AGE
                    && entry.repairs < 2
                    && entry
                        .last_repair
                        .is_none_or(|last| now.saturating_duration_since(last) >= REPAIR_INTERVAL)
            })
    }

    /// The first pending repair and its wire size.
    fn next_repair(&self, now: Instant) -> Option<(usize, &History, usize)> {
        self.repairs
            .iter()
            .enumerate()
            .find_map(|(index, sequence)| {
                let entry = self.repair_entry(*sequence, now)?;
                Some((index, entry, entry.packet.range.len() + WIRE_OVERHEAD + 2))
            })
    }

    pub fn deadline(&self, now: Instant) -> Option<Instant> {
        let primary = self.frames.front().map(|frame| {
            self.wire
                .ready(frame.range().len() + WIRE_OVERHEAD, now)
                .min(frame.deadline)
        });
        let repair = self.next_repair(now).map(|(_, _, bytes)| {
            self.wire
                .ready(bytes, now)
                .max(self.repair.ready(bytes, now))
        });
        primary.into_iter().chain(repair).min()
    }

    pub fn pop(&mut self, now: Instant) -> Option<Packet> {
        // The caller expires queued frames before popping so it can request a keyframe.
        if self
            .frames
            .front()
            .is_some_and(|frame| now >= frame.deadline)
        {
            return None;
        }
        self.trim_history(now);
        if let Some((index, entry, bytes)) = self.next_repair(now) {
            if self.wire.ready(bytes, now) <= now && self.repair.ready(bytes, now) <= now {
                let packet = Packet {
                    original_sequence: Some(entry.sequence),
                    data: entry.packet.data.clone(),
                    range: entry.packet.range.clone(),
                    ..entry.packet
                };
                self.repairs.remove(index);
                self.wire.spend(bytes, now);
                self.repair.spend(bytes, now);
                return Some(packet);
            }
        }
        let frame = self.frames.front_mut()?;
        let range = frame.range();
        let bytes = range.len() + WIRE_OVERHEAD;
        if self.wire.ready(bytes, now) > now {
            return None;
        }
        frame.next += 1;
        let marker = frame.next == frame.ends.len();
        let packet = Packet {
            timestamp: frame.timestamp,
            marker,
            original_sequence: None,
            data: frame.data.clone(),
            range,
        };
        if marker {
            if frame.keyframe {
                self.last_keyframe = Some(now);
            }
            self.frames.pop_front();
        }
        self.queued_bytes -= packet.range.len();
        self.queued_packets -= 1;
        self.wire.spend(bytes, now);
        Some(packet)
    }

    pub fn sent(&mut self, sequence: u16, packet: Packet, now: Instant) {
        self.trim_history(now);
        if let Some(original) = packet.original_sequence {
            if let Some(index) = self.history_index(original) {
                let entry = &mut self.history[index];
                entry.repairs += 1;
                entry.last_repair = Some(now);
            }
            return;
        }
        self.history_bytes += packet.range.len();
        self.history.push_back(History {
            sequence,
            packet,
            sent: now,
            repairs: 0,
            nacked: false,
            last_repair: None,
        });
        self.trim_history(now);
    }

    /// Queues repairs and returns how many sent packets were NACKed for the first time.
    pub fn request(&mut self, sequences: &[u16], now: Instant) -> usize {
        self.trim_history(now);
        let mut lost = 0;
        for &sequence in sequences {
            if let Some(index) = self.history_index(sequence) {
                lost += usize::from(!std::mem::replace(&mut self.history[index].nacked, true));
            }
            if self.repairs.len() < 64
                && !self.repairs.contains(&sequence)
                && self.repair_entry(sequence, now).is_some()
            {
                self.repairs.push_back(sequence);
            }
        }
        lost
    }

    pub fn clear(&mut self, now: Instant) {
        self.clear_frames();
        self.history.clear();
        self.history_bytes = 0;
        self.repairs.clear();
        self.last_keyframe = None;
        self.wire = Budget::new(self.wire.rate, now);
        self.repair = Budget::new(self.repair.rate, now);
    }
}
