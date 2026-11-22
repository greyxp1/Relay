//! H.264 Annex B parsing, RFC 6184 packetization and SPS normalization.
use super::pacer::Payloads;
use std::{borrow::Cow, mem::MaybeUninit};

#[allow(dead_code, non_camel_case_types, non_snake_case)]
mod parser {
    include!(concat!(env!("OUT_DIR"), "/h264.rs"));
}

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
                // Preserve the original Annex B prefix instead of the writer's four-byte prefix.
                out.extend_from_slice(&sps[4..]);
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

fn rewrite_sps(nal: &[u8]) -> Result<Option<Vec<u8>>> {
    use parser::*;

    const INVALID: &str = "Invalid or unsupported baseline H.264 SPS";
    if !(4..=4097).contains(&nal.len()) || nal[0] & 0x9f != 7 || nal[1] != 66 {
        return Err(INVALID.into());
    }
    // SAFETY: the parser reads the bounded NAL and initializes the SPS on success.
    let mut sps = unsafe {
        let mut unit: GstH264NalUnit = std::mem::zeroed();
        unit.data = nal.as_ptr().cast_mut();
        unit.size = nal.len() as u32;
        unit.header_bytes = 1;
        let mut sps = MaybeUninit::<GstH264SPS>::uninit();
        if gst_h264_parse_sps(&mut unit, sps.as_mut_ptr()) != GST_H264_PARSER_OK {
            return Err(INVALID.into());
        }
        sps.assume_init()
    };
    let vui = &mut sps.vui_parameters;
    if sps.num_ref_frames > 16
        || sps.pic_width_in_mbs_minus1 > 65535
        || sps.pic_height_in_map_units_minus1 > 65535
        || sps.frame_crop_left_offset > 65535
        || sps.frame_crop_right_offset > 65535
        || sps.frame_crop_top_offset > 65535
        || sps.frame_crop_bottom_offset > 65535
        || vui.max_bytes_per_pic_denom > 16
        || vui.num_reorder_frames > 16
        || vui.max_dec_frame_buffering > 16
    {
        return Err(INVALID.into());
    }
    if sps.vui_parameters_present_flag != 0
        && vui.bitstream_restriction_flag != 0
        && vui.num_reorder_frames == 0
        && vui.max_dec_frame_buffering <= sps.num_ref_frames
    {
        return Ok(None);
    }
    sps.vui_parameters_present_flag = 1;
    vui.bitstream_restriction_flag = 1;
    vui.motion_vectors_over_pic_boundaries_flag = 1;
    vui.max_bytes_per_pic_denom = 2;
    vui.max_bits_per_mb_denom = 1;
    vui.log2_max_mv_length_horizontal = 16;
    vui.log2_max_mv_length_vertical = 16;
    vui.num_reorder_frames = 0;
    vui.max_dec_frame_buffering = sps.num_ref_frames;

    // GStreamer 1.22 writes into the existing bits without clearing the buffer.
    let mut raw = [0u8; 8192];
    let mut raw_size = raw.len() as u32;
    let mut output = Vec::<u8>::with_capacity(raw.len());
    let mut size = output.capacity() as u32;
    // SAFETY: both writers get bounded buffers and report the lengths they initialized.
    unsafe {
        if gst_h264_bit_writer_sps(&sps, 0, raw.as_mut_ptr(), &mut raw_size)
            != GST_H264_BIT_WRITER_OK
            || !(3..=raw.len() as u32).contains(&raw_size)
        {
            return Err(INVALID.into());
        }
        raw[0] = nal[0];
        // Keep the constraint bits GStreamer's writer clears.
        raw[2] |= nal[2] & 3;
        if gst_h264_bit_writer_convert_to_nal(
            4,
            0,
            0,
            0,
            raw.as_ptr(),
            raw_size as gsize * 8,
            output.as_mut_ptr(),
            &mut size,
        ) != GST_H264_BIT_WRITER_OK
            || !(5..=output.capacity()).contains(&(size as usize))
        {
            return Err(INVALID.into());
        }
        output.set_len(size as usize);
    }
    Ok(Some(output))
}
