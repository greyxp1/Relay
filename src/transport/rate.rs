//! Send rate from REMB and NACKed loss, limited by Media Sink Wants.
use crate::capture::{VIDEO_MIN_BITRATE, VIDEO_START_BITRATE};
use std::time::{Duration, Instant};

/// Discord's Go Live minimum.
const MINIMUM: u32 = 500_000;
const RECEIVER_AGE: Duration = Duration::from_secs(5);
const WINDOW: Duration = Duration::from_secs(1);

pub(super) struct Rate {
    maximum: u32,
    /// Media Sink Wants limit.
    cap: u32,
    estimate: f64,
    reported: u32,
    receiver: Option<(u32, Instant)>,
    sent: u32,
    lost: u32,
    window: Instant,
}

impl Rate {
    pub fn new(max_bitrate: u32, now: Instant) -> Self {
        let maximum = max_bitrate.max(MINIMUM) / 1000 * 1000;
        let start = VIDEO_START_BITRATE.min(maximum);
        Self {
            maximum,
            cap: maximum,
            estimate: f64::from(start),
            reported: start,
            receiver: None,
            sent: 0,
            lost: 0,
            window: now,
        }
    }

    pub fn bitrate(&self) -> u32 {
        self.reported
    }

    fn receiver_bitrate(&self, now: Instant) -> Option<u32> {
        let fresh = self
            .receiver
            .filter(|(_, at)| now.saturating_duration_since(*at) < RECEIVER_AGE);
        fresh.map(|(bitrate, _)| bitrate.max(VIDEO_MIN_BITRATE))
    }

    /// Reports changes of at least 5 percent, and any change at the ceiling.
    fn apply(&mut self, now: Instant) -> Option<u32> {
        let ceiling = self
            .maximum
            .min(self.cap)
            .min(self.receiver_bitrate(now).unwrap_or(u32::MAX));
        self.estimate = self
            .estimate
            .min(f64::from(ceiling))
            .max(f64::from(MINIMUM.min(ceiling)));
        let next = self.estimate as u32 / 1000 * 1000;
        let changed = next.abs_diff(self.reported) * 20 >= self.reported
            || (next != self.reported && next == ceiling / 1000 * 1000);
        changed.then(|| {
            self.reported = next;
            next
        })
    }

    pub fn receiver(&mut self, bitrate: u32, now: Instant) -> Option<u32> {
        self.receiver = Some((bitrate, now));
        self.apply(now)
    }

    /// Caps the rate for a Media Sink Wants request.
    pub fn set_cap(&mut self, cap: u32, now: Instant) -> Option<u32> {
        self.cap = cap.clamp(MINIMUM.min(self.maximum), self.maximum);
        self.apply(now)
    }

    pub fn sent(&mut self) {
        self.sent = self.sent.saturating_add(1);
    }

    pub fn lost(&mut self, packets: usize) {
        self.lost = self.lost.saturating_add(packets as u32);
    }

    /// Once per window, backs off for loss above 10 percent and otherwise grows
    /// toward REMB, or by 8 percent without one, while loss stays under 2 percent.
    pub fn update(&mut self, now: Instant) -> Option<u32> {
        if now.saturating_duration_since(self.window) < WINDOW {
            return None;
        }
        self.window = now;
        let (sent, lost) = (
            std::mem::take(&mut self.sent),
            std::mem::take(&mut self.lost),
        );
        // Too little traffic is no evidence either way.
        if sent < 20 {
            return None;
        }
        let loss = f64::from(lost.min(sent)) / f64::from(sent);
        if loss > 0.1 {
            self.estimate *= 1.0 - 0.5 * loss;
        } else if loss < 0.02 {
            let receiver = self.receiver_bitrate(now).map_or(0.0, f64::from);
            self.estimate = receiver.max(self.estimate * 1.08);
        }
        self.apply(now)
    }

    /// Forgets loss counts, keeping the estimate.
    pub fn clear(&mut self) {
        self.sent = 0;
        self.lost = 0;
    }
}
