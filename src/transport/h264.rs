//! H.264 Annex B parsing, RFC 6184 packetization and SPS normalization.
use std::borrow::Cow;

fn valid(nal: &[u8]) -> bool {
    nal.first()
        .is_some_and(|&header| header & 0x80 == 0 && (1..=23).contains(&(header & 31)))
}

/// Annex B NAL unit ranges, each checked by `valid`.
pub(super) fn nals(frame: &[u8], valid: fn(&[u8]) -> bool) -> Result<Vec<(usize, usize)>> {
    let mut nals: Vec<(usize, usize)> = Vec::new();
    for i in memchr::memmem::find_iter(frame, &[0, 0, 1]) {
        let prefix = if i > 0 && frame[i - 1] == 0 { i - 1 } else { i };
        if let Some((_, end)) = nals.last_mut() {
            *end = prefix;
        } else if prefix != 0 {
            return Err("Video frame is not Annex B".into());
        }
        nals.push((i + 3, frame.len()));
    }
    if nals.is_empty() {
        return Err("Video frame is not Annex B".into());
    }
    for &(start, end) in &nals {
        if !valid(&frame[start..end]) {
            return Err("Invalid NAL unit".into());
        }
    }
    Ok(nals)
}

/// RFC 6184 single NAL units and FU-A fragments from one Annex B frame.
pub(super) fn payloads(frame: &[u8], limit: usize) -> Result<Vec<Vec<u8>>> {
    let mut payloads = Vec::new();
    for (start, end) in nals(frame, valid)? {
        let nal = &frame[start..end];
        let (&header, body) = nal.split_first().unwrap();
        if nal.len() <= limit {
            payloads.push(nal.to_vec());
        } else {
            let chunks = body.len().div_ceil(limit - 2);
            for (part, chunk) in body.chunks(limit - 2).enumerate() {
                let mut payload = vec![
                    (header & 0xe0) | 28,
                    (header & 31)
                        | if part == 0 { 0x80 } else { 0 }
                        | if part + 1 == chunks { 0x40 } else { 0 },
                ];
                payload.extend_from_slice(chunk);
                payloads.push(payload);
            }
        }
    }
    Ok(payloads)
}

/// Keep baseline SPS bytes stable through WebRTC's receive-side VUI rewriter.
pub(super) fn normalize(frame: &[u8]) -> Result<Cow<'_, [u8]>> {
    let mut output = None;
    let mut copied = 0;
    for (start, end) in nals(frame, valid)? {
        if frame[start] & 31 != 7 {
            continue;
        }
        if let Some(sps) = rewrite_sps(&frame[start..end])? {
            let out = output.get_or_insert_with(|| Vec::with_capacity(frame.len() + 16));
            out.extend_from_slice(&frame[copied..start]);
            out.extend(sps);
            copied = end;
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

const INVALID: &str = "Invalid or unsupported baseline H.264 SPS";
type Result<T> = std::result::Result<T, String>;

unsafe extern "C" {
    fn relay_normalize_sps(
        input: *const u8,
        size: u32,
        output: *mut u8,
        output_size: *mut u32,
    ) -> i32;
}

fn rewrite_sps(nal: &[u8]) -> Result<Option<Vec<u8>>> {
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
