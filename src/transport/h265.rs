//! RFC 7798 single NAL unit and FU packetization of an Annex B frame.
use super::{h264::packetize, pacer::Payloads};

/// Forbidden bit clear, nonzero TemporalId, and no RTP-only types.
fn valid(nal: &[u8]) -> bool {
    nal.len() > 2 && nal[0] & 0x80 == 0 && (nal[0] >> 1) < 48 && nal[1] & 7 != 0
}

pub(super) fn payloads(frame: &[u8], limit: usize) -> Result<Payloads, String> {
    packetize(frame, limit, 2, valid)
}
