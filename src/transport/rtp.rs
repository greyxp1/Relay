//! Discord AES256-GCM rtpsize, RTP, and bounded RTCP feedback parsing.
use crate::capture::Codec;
use aes_gcm::{
    aead::{AeadInPlace, KeyInit},
    Aes256Gcm, Nonce, Tag,
};
use std::time::{SystemTime, UNIX_EPOCH};

pub(super) const MODE: &str = "aead_aes256_gcm_rtpsize";
// Leave room for UDP/IP and Discord's forwarding overhead on minimum-MTU paths.
pub(super) const MTU: usize = 1200;
pub(super) const OPUS_PT: u8 = 120;

/// Media and RTX payload types advertised in Select Protocol. 96 is reserved for probes.
pub(super) fn payload_types(codec: Codec) -> (u8, u8) {
    match codec {
        Codec::H265 => (103, 104),
        Codec::Av1 => (109, 110),
        _ => (101, 102),
    }
}

fn now() -> std::time::Duration {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
}

#[derive(Default)]
pub(super) struct Feedback {
    pub keyframe: bool,
    pub nacks: Vec<u16>,
    pub receiver_bitrate: Option<u32>,
}

/// Parse authenticated compound or reduced-size RTCP, rejecting the whole
/// datagram if any packet is malformed (RFC 3550, 4585 and 5506).
pub(super) fn feedback(mut packet: &[u8], video_ssrc: u32) -> Option<Feedback> {
    if packet.is_empty() || packet.len() > 4096 {
        return None;
    }
    let mut feedback = Feedback::default();
    let ssrc = video_ssrc.to_be_bytes();
    while !packet.is_empty() {
        if packet.len() < 4 || packet[0] >> 6 != 2 {
            return None;
        }
        let size = (usize::from(u16::from_be_bytes([packet[2], packet[3]])) + 1) * 4;
        if size > packet.len() {
            return None;
        }
        let mut part = &packet[..size];
        packet = &packet[size..];
        if part[0] & 32 != 0 {
            let padding = usize::from(*part.last()?);
            // Discord can append NACK after a padded TWCC subpacket. Each
            // declared subpacket boundary still determines its own padding.
            if padding == 0 || padding > size - 4 {
                return None;
            }
            part = &part[..size - padding];
        }
        let count = usize::from(part[0] & 31);
        match (part[1], count) {
            (200 | 201, _) => {
                let end = if part[1] == 200 { 28 } else { 8 } + count * 24;
                // SR/RR may carry aligned profile-specific extensions.
                if part.len() < end || !(part.len() - end).is_multiple_of(4) {
                    return None;
                }
            }
            (205 | 206, _) if part.len() < 12 => return None,
            (205, 1) => {
                if part.len() < 16 || !(part.len() - 12).is_multiple_of(4) {
                    return None;
                }
                if part[8..12] != ssrc {
                    continue;
                }
                for nack in part[12..].as_chunks::<4>().0 {
                    let pid = u16::from_be_bytes([nack[0], nack[1]]);
                    let mask = u16::from_be_bytes([nack[2], nack[3]]);
                    for bit in (0..=16).filter(|b| *b == 0 || mask & (1 << (b - 1)) != 0) {
                        let sequence = pid.wrapping_add(bit);
                        if feedback.nacks.len() < 64 && !feedback.nacks.contains(&sequence) {
                            feedback.nacks.push(sequence);
                        }
                    }
                }
            }
            (206, 15) if part.get(12..16) == Some(b"REMB") => {
                if part.len() < 20 || part.len() != 20 + usize::from(part[16]) * 4 {
                    return None;
                }
                if part[20..].as_chunks::<4>().0.contains(&ssrc) {
                    let mantissa =
                        u64::from(u32::from_be_bytes([0, part[17] & 3, part[18], part[19]]));
                    let bitrate = mantissa
                        .saturating_mul(1 << (part[17] >> 2))
                        .min(u32::MAX.into()) as u32;
                    let old = feedback.receiver_bitrate.unwrap_or(u32::MAX);
                    feedback.receiver_bitrate = Some(old.min(bitrate));
                }
            }
            (206, 1) => {
                if part.len() != 12 {
                    return None;
                }
                feedback.keyframe |= part[8..12] == ssrc;
            }
            _ => {}
        }
    }
    Some(feedback)
}

pub(super) struct Cipher {
    aes: Aes256Gcm,
    // Shared by RTP and RTCP. A new key is mandatory before exhaustion.
    counter: u32,
    scratch: Vec<u8>,
}
impl Cipher {
    pub(super) fn new(key: &[u8]) -> Result<Self, String> {
        Ok(Self {
            aes: Aes256Gcm::new_from_slice(key).map_err(|_| "Invalid transport key")?,
            counter: 0,
            scratch: Vec::with_capacity(MTU + 64),
        })
    }
    pub(super) fn seal(&mut self, header: &[u8], data: &[&[u8]]) -> Result<&[u8], String> {
        let count = self.counter;
        self.counter = count.checked_add(1).ok_or("Transport nonce exhausted")?;
        let mut nonce = [0; 12];
        nonce[..4].copy_from_slice(&count.to_be_bytes());
        self.scratch.clear();
        self.scratch.extend_from_slice(header);
        for part in data {
            self.scratch.extend_from_slice(part);
        }
        let tag = self
            .aes
            .encrypt_in_place_detached(
                Nonce::from_slice(&nonce),
                header,
                &mut self.scratch[header.len()..],
            )
            .map_err(|_| "Transport encryption failed")?;
        self.scratch.extend_from_slice(&tag);
        self.scratch.extend(count.to_be_bytes());
        Ok(&self.scratch)
    }
    pub(super) fn open<'a>(&self, packet: &'a mut [u8], clear: usize) -> Option<&'a [u8]> {
        let end = packet.len().checked_sub(20).filter(|end| clear <= *end)?;
        let mut nonce = [0; 12];
        nonce[..4].copy_from_slice(&packet[end + 16..]);
        let (header, encrypted) = packet[..end + 16].split_at_mut(clear);
        let (payload, tag) = encrypted.split_at_mut(end - clear);
        let tag = Tag::from_slice(tag);
        self.aes
            .decrypt_in_place_detached(Nonce::from_slice(&nonce), header, payload, tag)
            .ok()?;
        Some(&packet[..end])
    }
}

pub(super) fn header(sequence: u16, timestamp: u32, ssrc: u32, marker: bool, kind: u8) -> [u8; 12] {
    let mut h = [
        0x80,
        kind | u8::from(marker) << 7,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        0,
    ];
    h[2..4].copy_from_slice(&sequence.to_be_bytes());
    h[4..8].copy_from_slice(&timestamp.to_be_bytes());
    h[8..12].copy_from_slice(&ssrc.to_be_bytes());
    h
}

/// Video RTP header and its length. RFC 8285 one-byte elements in Discord's
/// sender-side numbering: transport sequence (5) and abs-send-time (3), then
/// screen content (7) and RID "100" (11) unless repairing. RTX keeps DAVE
/// ciphertext and appends the original sequence.
pub(super) fn video_header(
    sequence: u16,
    ssrc: u32,
    codec: Codec,
    transport: u16,
    packet: &super::pacer::Packet,
) -> ([u8; 34], usize) {
    let (media, rtx) = payload_types(codec);
    let original = packet.original_sequence;
    let size = if original.is_some() { 8 } else { 16 };
    let kind = if original.is_some() { rtx } else { media };
    let mut h = [0; 34];
    h[..12].copy_from_slice(&header(
        sequence,
        packet.timestamp,
        ssrc,
        packet.marker,
        kind,
    ));
    h[0] |= 0x10;
    let now = now();
    // 6.18 fixed point seconds.
    let send =
        ((u64::from(now.subsec_nanos()) << 18) / 1_000_000_000) | ((now.as_secs() & 63) << 18);
    let [_, _, _, _, _, a, b, c] = send.to_be_bytes();
    let [high, low] = transport.to_be_bytes();
    h[12..23].copy_from_slice(&[
        0xbe,
        0xde,
        0,
        size as u8 / 4,
        0x51,
        high,
        low,
        0x32,
        a,
        b,
        c,
    ]);
    match original {
        Some(original) => h[24..26].copy_from_slice(&original.to_be_bytes()),
        None => h[23..29].copy_from_slice(&[0x70, 1, 0xb2, b'1', b'0', b'0']),
    }
    (h, 16 + size + usize::from(original.is_some()) * 2)
}

/// Header length of an encrypted RTP packet. The extension preamble is
/// authenticated cleartext while its content is encrypted.
pub(super) fn clear_len(packet: &[u8]) -> Option<usize> {
    if packet.len() < 12 || packet[0] >> 6 != 2 {
        return None;
    }
    let size = 12 + usize::from(packet[0] & 15) * 4 + if packet[0] & 16 != 0 { 4 } else { 0 };
    (packet.len() >= size).then_some(size)
}

/// The Opus payload of a decrypted packet without extension content or padding.
pub(super) fn opus<'a>(header: &[u8], plain: &'a [u8]) -> Option<&'a [u8]> {
    if header[1] & 127 != OPUS_PT {
        return None;
    }
    let start = if header[0] & 16 != 0 {
        usize::from(u16::from_be_bytes([
            header[header.len() - 2],
            header[header.len() - 1],
        ])) * 4
    } else {
        0
    };
    let padding = if header[0] & 32 != 0 {
        usize::from(*plain.last().filter(|p| **p != 0)?)
    } else {
        0
    };
    plain
        .get(start..plain.len().checked_sub(padding)?)
        .filter(|p| !p.is_empty())
}

pub(super) fn sender_report(ssrc: u32, timestamp: u32, packets: u64, octets: u64) -> Vec<u8> {
    let now = now();
    let mut report = vec![0x80, 200, 0, 6];
    report.extend(ssrc.to_be_bytes());
    report.extend((now.as_secs().wrapping_add(2_208_988_800) as u32).to_be_bytes());
    report.extend((((u64::from(now.subsec_nanos()) << 32) / 1_000_000_000) as u32).to_be_bytes());
    report.extend(timestamp.to_be_bytes());
    report.extend((packets as u32).to_be_bytes());
    report.extend((octets as u32).to_be_bytes());
    // Compound RTCP requires an SDES CNAME. This reveals no account identifier.
    report.extend([0x81, 202, 0, 3]);
    report.extend(ssrc.to_be_bytes());
    report.extend(*b"\x01\x05relay\0");
    report
}

pub(super) fn receiver_report(ssrc: u32) -> [u8; 8] {
    let [a, b, c, d] = ssrc.to_be_bytes();
    [0x80, 201, 0, 1, a, b, c, d]
}
