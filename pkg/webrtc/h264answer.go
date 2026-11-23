package webrtc

import (
	"encoding/base64"
	"encoding/hex"
	"strings"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/pion/webrtc/v4"
)

// When go2rtc answers a WebRTC offer (browser, or another go2rtc relaying
// this stream), the remote offers several H.264 variants and pion would
// answer with all of them in offer order. A remote go2rtc then takes the
// first one, usually profile-level-id=42001f (Baseline), and labels its MSE
// output "avc1.42001F" even when the source is 4K Main or High. Firefox
// configures its decoder from that label and fails or stalls on the real
// stream (seen 2026-11-23 with Frigate relaying exp's cameras).
//
// A WebRTC SDP never carries the source's parameter sets, but go2rtc knows
// them for most sources (RTSP cameras put them in their own SDP). So the
// answer is narrowed to one H.264 entry:
//   - from the offered list, the entry whose profile family matches the
//     source (Main is decodable by High decoders, so High is the fallback),
//   - with the source's sprop-parameter-sets appended, which go2rtc's
//     h264.GetProfileLevelID reads before profile-level-id.
// Browsers ignore sprop-parameter-sets; pion matches H.264 entries only on
// packetization-mode and the first two bytes of profile-level-id.

// sourceProfileIDC returns the H.264 profile_idc of a source codec, from its
// SPS when known, else from its profile-level-id.
func sourceProfileIDC(src *core.Codec) (byte, bool) {
	if src == nil || src.Name != core.CodecH264 {
		return 0, false
	}
	if sps, _ := h264.GetParameterSet(src.FmtpLine); len(sps) >= 2 {
		return sps[1], true
	}
	if s := core.Between(src.FmtpLine, "profile-level-id=", ";"); len(s) >= 2 {
		if b, err := hex.DecodeString(s[:2]); err == nil {
			return b[0], true
		}
	}
	return 0, false
}

func offeredProfileIDC(c *core.Codec) (byte, bool) {
	s := core.Between(c.FmtpLine, "profile-level-id=", ";")
	if len(s) < 2 {
		return 0, false
	}
	b, err := hex.DecodeString(s[:2])
	if err != nil {
		return 0, false
	}
	return b[0], true
}

func packetizationMode(fmtp string) string {
	return core.Between(fmtp, "packetization-mode=", ";")
}

// chooseH264 picks, among the H.264 codecs the remote offered for media, the
// one closest to the source's profile. It only considers entries with the
// same packetization-mode as the codec the stream matched, and falls back to
// that codec when nothing better was offered.
func chooseH264(media *core.Media, matched, src *core.Codec) *core.Codec {
	want, ok := sourceProfileIDC(src)
	if !ok || media == nil {
		return matched
	}
	pm := packetizationMode(matched.FmtpLine)

	find := func(idc byte) *core.Codec {
		for _, c := range media.Codecs {
			if c.Name != core.CodecH264 || packetizationMode(c.FmtpLine) != pm {
				continue
			}
			if got, ok := offeredProfileIDC(c); ok && got == idc {
				return c
			}
		}
		return nil
	}

	if c := find(want); c != nil {
		return c
	}
	// Main (77) and Extended (88) streams decode on a High (100) decoder.
	if want == h264.ProfileMain || want == 88 {
		if c := find(h264.ProfileHigh); c != nil {
			return c
		}
	}
	return matched
}

// answerH264Codec builds the single H.264 entry the answer is narrowed to:
// the chosen offered entry (same payload type and fmtp) plus the source's
// sprop-parameter-sets when the source has both SPS and PPS.
func answerH264Codec(chosen, src *core.Codec) webrtc.RTPCodecParameters {
	fmtp := chosen.FmtpLine
	if sps, pps := h264.GetParameterSet(src.FmtpLine); len(sps) > 0 && len(pps) > 0 &&
		!strings.Contains(fmtp, "sprop-parameter-sets=") {
		fmtp += ";sprop-parameter-sets=" + base64.StdEncoding.EncodeToString(sps) +
			"," + base64.StdEncoding.EncodeToString(pps)
	}
	return webrtc.RTPCodecParameters{
		RTPCodecCapability: webrtc.RTPCodecCapability{
			MimeType:     webrtc.MimeTypeH264,
			ClockRate:    90000,
			SDPFmtpLine:  fmtp,
			RTCPFeedback: videoRTCPFeedback,
		},
		PayloadType: webrtc.PayloadType(chosen.PayloadType),
	}
}
