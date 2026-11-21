//! AV1 RTP payload format packetization of a low overhead OBU stream.
//! DAVE leaves every size field but the last, which also covers its trailer.
use super::pacer::Payloads;
use std::borrow::Cow;

fn leb128(mut value: usize, out: &mut Vec<u8>) {
    while value > 127 {
        out.push(value as u8 | 0x80);
        value >>= 7;
    }
    out.push(value as u8);
}

fn leb128_len(value: usize) -> usize {
    (usize::BITS - value.max(1).leading_zeros()).div_ceil(7) as usize
}

/// OBUs without size fields, minus temporal delimiters, tile lists and padding.
fn obus(frame: &[u8]) -> Result<Vec<Cow<'_, [u8]>>, String> {
    const INVALID: &str = "Invalid AV1 OBU";
    let mut obus = Vec::new();
    let mut at = 0;
    while at < frame.len() {
        let header = frame[at];
        let extension = header & 4 != 0;
        // Forbidden and reserved bits.
        if header & 0x81 != 0 {
            return Err(INVALID.into());
        }
        let mut start = at + 1 + usize::from(extension);
        let end = if header & 2 != 0 {
            let (mut size, mut shift) = (0, 0);
            loop {
                let byte = *frame.get(start).ok_or(INVALID)?;
                size |= usize::from(byte & 127) << shift;
                start += 1;
                shift += 7;
                if byte & 128 == 0 {
                    break;
                }
                if shift > 28 {
                    return Err(INVALID.into());
                }
            }
            start + size
        } else {
            frame.len()
        };
        if start > end || end > frame.len() {
            return Err(INVALID.into());
        }
        if !matches!((header >> 3) & 15, 2 | 8 | 15) {
            obus.push(if header & 2 == 0 {
                Cow::Borrowed(&frame[at..end])
            } else {
                let mut obu = vec![header & !2];
                obu.extend_from_slice(&frame[at + 1..at + 1 + usize::from(extension)]);
                obu.extend_from_slice(&frame[start..end]);
                Cow::Owned(obu)
            });
        }
        at = end;
    }
    if obus.is_empty() {
        return Err("Empty AV1 frame".into());
    }
    Ok(obus)
}

/// Packets of whole or fragmented OBU elements. A single element omits its length.
pub(super) fn payloads(frame: &[u8], keyframe: bool, limit: usize) -> Result<Payloads, String> {
    let obus = obus(frame)?;
    // N marks a keyframe that starts with its sequence header.
    let mut first = if keyframe && (obus[0][0] >> 3) & 15 == 1 {
        8
    } else {
        0
    };
    let mut payloads = Payloads::default();
    // Elements of the open packet: bytes, continued from and continuing into another packet.
    let mut elements: Vec<(&[u8], bool, bool)> = Vec::new();
    let mut used = 1;
    let mut flush = |elements: &mut Vec<(&[u8], bool, bool)>, used: &mut usize| {
        let single = elements.len() == 1;
        let payload = payloads.open();
        payload.push(
            first
                | if elements.first().is_some_and(|e| e.1) {
                    0x80
                } else {
                    0
                }
                | if elements.last().is_some_and(|e| e.2) {
                    0x40
                } else {
                    0
                }
                | if single { 0x10 } else { 0 },
        );
        for (element, ..) in elements.drain(..) {
            if !single {
                leb128(element.len(), payload);
            }
            payload.extend_from_slice(element);
        }
        payloads.close();
        first = 0;
        *used = 1;
    };
    for obu in &obus {
        let mut offset = 0;
        while offset < obu.len() {
            let room = limit - used;
            if room < 4 {
                flush(&mut elements, &mut used);
                continue;
            }
            let take = (obu.len() - offset).min(room - leb128_len(room));
            elements.push((
                &obu[offset..offset + take],
                offset > 0,
                offset + take < obu.len(),
            ));
            used += leb128_len(take) + take;
            offset += take;
            // A fragment must be the last element of its packet.
            if offset < obu.len() {
                flush(&mut elements, &mut used);
            }
        }
    }
    if !elements.is_empty() {
        flush(&mut elements, &mut used);
    }
    Ok(payloads)
}
