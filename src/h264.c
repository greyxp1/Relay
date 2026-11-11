#define GST_USE_UNSTABLE_API
#include <gst/codecparsers/gsth264parser.h>
#include <gst/codecparsers/gsth264bitwriter.h>
#include <string.h>

static int encode(const GstH264SPS *sps, const unsigned char *input,
                  unsigned char *output, unsigned *size) {
    unsigned char raw[8192];
    unsigned raw_size = sizeof raw;
    if (gst_h264_bit_writer_sps(sps, FALSE, raw, &raw_size) != GST_H264_BIT_WRITER_OK)
        return 0;
    raw[0] = input[0];
    raw[2] |= input[2] & 3; /* Keep constraint bits that the native writer clears. */
    return gst_h264_bit_writer_convert_to_nal(4, FALSE, FALSE, FALSE,
        raw, raw_size * 8, output, size) == GST_H264_BIT_WRITER_OK;
}

/* Input includes the SPS header. Output includes a four-byte Annex B prefix.
 * Caller owns output. -1 rejects input, 0 leaves it untouched, 1 rewrites it. */
int relay_normalize_sps(const unsigned char *input, unsigned size,
                        unsigned char *output, unsigned *output_size) {
    if (size < 4 || size > 4097 || (input[0] & 0x9f) != 7 || input[1] != 66)
        return -1;
    GstH264NalUnit nal = {.data = (guint8 *)input, .size = size, .header_bytes = 1};
    GstH264SPS sps;
    if (gst_h264_parse_sps(&nal, &sps) != GST_H264_PARSER_OK)
        return -1;
    GstH264VUIParams *vui = &sps.vui_parameters;
    if (sps.num_ref_frames > 16 || sps.pic_width_in_mbs_minus1 > 65535 ||
        sps.pic_height_in_map_units_minus1 > 65535 ||
        sps.frame_crop_left_offset > 65535 || sps.frame_crop_right_offset > 65535 ||
        sps.frame_crop_top_offset > 65535 || sps.frame_crop_bottom_offset > 65535 ||
        vui->max_bytes_per_pic_denom > 16 || vui->num_reorder_frames > 16 ||
        vui->max_dec_frame_buffering > 16)
        return -1;
    if (sps.vui_parameters_present_flag && vui->bitstream_restriction_flag &&
        vui->num_reorder_frames == 0 && vui->max_dec_frame_buffering <= sps.num_ref_frames)
        return 0;
    sps.vui_parameters_present_flag = 1;
    vui->bitstream_restriction_flag = 1;
    vui->motion_vectors_over_pic_boundaries_flag = 1;
    vui->max_bytes_per_pic_denom = 2;
    vui->max_bits_per_mb_denom = 1;
    vui->log2_max_mv_length_horizontal = 16;
    vui->log2_max_mv_length_vertical = 16;
    vui->num_reorder_frames = 0;
    vui->max_dec_frame_buffering = sps.num_ref_frames;
    return encode(&sps, input, output, output_size) ? 1 : -1;
}
