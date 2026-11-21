//! H.264 Annex B parsing, RFC 6184 packetization and SPS normalization.
use super::pacer::Payloads;
use std::borrow::Cow;

type Result<T> = std::result::Result<T, String>;

fn valid(nal: &[u8]) -> bool {
    nal.first()
        .is_some_and(|&header| header & 0x80 == 0 && (1..=23).contains(&(header & 31)))
}

/// Annex B NAL units, each checked by `valid`.
pub(super) fn nals(frame: &[u8], valid: fn(&[u8]) -> bool) -> Result<Vec<(usize, usize)>> {
    let mut nals: Vec<(usize, usize)> = Vec::new();
    for i in memchr::memmem::find_iter(frame, &[0, 0, 1]) {
        let prefix = if i > 0 && frame[i - 1] == 0 { i - 1 } else { i };
        match nals.last_mut() {
            Some((_, end)) => *end = prefix,
            None if prefix != 0 => return Err("Video frame is not Annex B".into()),
            None => {}
        }
        nals.push((i + 3, frame.len()));
    }
    if nals.is_empty() || !nals.iter().all(|&(start, end)| valid(&frame[start..end])) {
        return Err("Invalid Annex B video frame".into());
    }
    Ok(nals)
}

/// RFC 6184 single NAL units and FU-A fragments, or RFC 7798 for H.265 with a
/// two-byte header, splitting NAL units that exceed `limit`.
pub(super) fn packetize(
    frame: &[u8],
    limit: usize,
    header: usize,
    valid: fn(&[u8]) -> bool,
) -> Result<Payloads> {
    let mut payloads = Payloads::default();
    for (start, end) in nals(frame, valid)? {
        let nal = &frame[start..end];
        if nal.len() <= limit {
            payloads.push(&[nal]);
            continue;
        }
        let (fu, kind): ([u8; 2], u8) = if header == 1 {
            ([(nal[0] & 0xe0) | 28, 0], nal[0] & 31)
        } else {
            // The FU payload header keeps F, LayerId and TID from the NAL header.
            ([(nal[0] & 0x81) | (49 << 1), nal[1]], (nal[0] >> 1) & 63)
        };
        let chunks = (nal.len() - header).div_ceil(limit - header - 1);
        for (part, chunk) in nal[header..].chunks(limit - header - 1).enumerate() {
            let edge = if part == 0 { 0x80 } else { 0 } | if part + 1 == chunks { 0x40 } else { 0 };
            payloads.push(&[&fu[..header], &[kind | edge], chunk]);
        }
    }
    Ok(payloads)
}

pub(super) fn payloads(frame: &[u8], limit: usize) -> Result<Payloads> {
    packetize(frame, limit, 1, valid)
}

/// Keep baseline SPS bytes stable through WebRTC's receive-side VUI rewriter.
pub(super) fn normalize(frame: &[u8]) -> Result<Cow<'_, [u8]>> {
    let mut output = None;
    let mut copied = 0;
    for (start, end) in nals(frame, valid)? {
        if frame[start] & 31 == 7 {
            if let Some(sps) = rewrite_sps(&frame[start..end])? {
                let out = output.get_or_insert_with(|| Vec::with_capacity(frame.len() + 16));
                out.extend_from_slice(&frame[copied..start]);
                out.extend(sps);
                copied = end;
            }
        }
    }
    Ok(match output {
        Some(mut out) => {
            out.extend_from_slice(&frame[copied..]);
            Cow::Owned(out)
        }
        None => Cow::Borrowed(frame),
    })
}

unsafe extern "C" {
    fn relay_normalize_sps(
        input: *const u8,
        size: u32,
        output: *mut u8,
        output_size: *mut u32,
    ) -> i32;
}

fn rewrite_sps(nal: &[u8]) -> Result<Option<Vec<u8>>> {
    const INVALID: &str = "Invalid or unsupported baseline H.264 SPS";
    if nal.len() > 4097 {
        return Err(INVALID.into());
    }
    let mut output = Vec::<u8>::with_capacity(8192);
    let mut size = output.capacity() as u32;
    // SAFETY: both buffers are bounded and the native writer reports its initialized length.
    let result = unsafe {
        relay_normalize_sps(
            nal.as_ptr(),
            nal.len() as u32,
            output.as_mut_ptr(),
            &mut size,
        )
    };
    match result {
        0 => Ok(None),
        1 if (5..=output.capacity()).contains(&(size as usize)) => {
            // SAFETY: successful native conversion initialized the returned Annex B NAL.
            unsafe { output.set_len(size as usize) };
            output.drain(..4);
            Ok(Some(output))
        }
        _ => Err(INVALID.into()),
    }
}
