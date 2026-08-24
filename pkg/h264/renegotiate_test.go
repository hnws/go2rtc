package h264

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

// avccNAL builds a single length-prefixed AVCC NAL unit for tests.
func avccNAL(naluType byte, filler ...byte) []byte {
	return JoinNALU(append([]byte{naluType}, filler...))
}

// TestRTPDepay_LearnsFreshParameterSet covers the bug behind
// https://github.com/AlexxIT/go2rtc "non-existing PPS 0 referenced / no
// frame!" reports: a source that changes (or never advertised) its
// sprop-parameter-sets and instead sends SPS/PPS in-band must have keyframes
// repaired with what it's actually sending, not a stale/empty value frozen
// from the original SDP offer/answer.
func TestRTPDepay_LearnsFreshParameterSet(t *testing.T) {
	sps := avccNAL(NALUTypeSPS, 0x11, 0x22, 0x33)
	pps := avccNAL(NALUTypePPS, 0x44)
	iframe := avccNAL(NALUTypeIFrame, 0xAA, 0xBB, 0xCC, 0xDD)

	// producer never advertised sprop-parameter-sets in its SDP
	codec := &core.Codec{Name: core.CodecH264, FmtpLine: ""}

	var got []byte
	depay := RTPDepay(codec, func(packet *rtp.Packet) {
		got = append([]byte(nil), packet.Payload...)
	})
	pay := RTPPay(0, depay)

	send := func(nal []byte, ts uint32) {
		pay(&rtp.Packet{
			Header:  rtp.Header{Timestamp: ts, Version: RTPPacketVersionAVC},
			Payload: nal,
		})
	}

	// 1. First keyframe, nothing negotiated and nothing seen yet - goes out
	// bare, since there's no parameter set to repair it with.
	got = nil
	send(iframe, 1)
	require.Equal(t, iframe, got)

	// 2. Source starts sending real SPS/PPS in-band.
	got = nil
	send(sps, 2)
	send(pps, 2)
	send(iframe, 2)
	require.Equal(t, JoinNALU(sps[4:], pps[4:], iframe[4:]), got)

	// 3. A later keyframe omits its own parameter set again - it must be
	// repaired with what was actually learned in step 2, not the empty
	// value from the original negotiation.
	got = nil
	send(iframe, 3)
	require.Equal(t, JoinNALU(sps[4:], pps[4:], iframe[4:]), got)
}

// TestRepairAVCC_LearnsFreshParameterSet is the same scenario for producers
// that deliver AVCC directly instead of RTP (track.Codec.IsRTP() == false).
func TestRepairAVCC_LearnsFreshParameterSet(t *testing.T) {
	sps := avccNAL(NALUTypeSPS, 0x11, 0x22, 0x33)
	pps := avccNAL(NALUTypePPS, 0x44)
	iframe := avccNAL(NALUTypeIFrame, 0xAA, 0xBB, 0xCC, 0xDD)

	codec := &core.Codec{Name: core.CodecH264, FmtpLine: ""}

	var got []byte
	fn := RepairAVCC(codec, func(packet *rtp.Packet) {
		got = append([]byte(nil), packet.Payload...)
	})

	send := func(nal []byte) {
		fn(&rtp.Packet{Payload: append([]byte(nil), nal...)})
	}

	// 1. No parameter set known yet - keyframe goes out bare.
	send(iframe)
	require.Equal(t, iframe, got)

	// 2. Source sends real SPS/PPS in-band, each as its own AVCC unit.
	send(sps)
	send(pps)

	// 3. A later keyframe with no parameter set of its own must now be
	// repaired with the freshly learned SPS/PPS.
	send(iframe)
	require.Equal(t, Join(JoinNALU(sps[4:], pps[4:]), iframe), got)
}

// TestRTPDepay_IgnoresPartialParameterSet covers a source that sends SPS
// once (e.g. at the very start of the underlying producer connection, well
// before this particular consumer ever attached) and only repeats PPS with
// each keyframe afterwards. A consumer that only ever observes the repeated
// PPS must not build ps from a PPS with no SPS - that's worse than leaving
// the keyframe bare, since it hands the decoder a parameter set reference
// that can never resolve.
func TestRTPDepay_IgnoresPartialParameterSet(t *testing.T) {
	pps := avccNAL(NALUTypePPS, 0x44)
	iframe := avccNAL(NALUTypeIFrame, 0xAA, 0xBB, 0xCC, 0xDD)

	codec := &core.Codec{Name: core.CodecH264, FmtpLine: ""}

	var got []byte
	depay := RTPDepay(codec, func(packet *rtp.Packet) {
		got = append([]byte(nil), packet.Payload...)
	})
	pay := RTPPay(0, depay)

	send := func(nal []byte, ts uint32) {
		pay(&rtp.Packet{
			Header:  rtp.Header{Timestamp: ts, Version: RTPPacketVersionAVC},
			Payload: nal,
		})
	}

	// PPS repeats, but SPS is never seen by this consumer.
	got = nil
	send(pps, 1)
	send(iframe, 1)
	require.Equal(t, iframe, got, "must not inject a PPS with no matching SPS")
}

// TestRepairAVCC_IgnoresPartialParameterSet is the AVCC-producer equivalent
// of TestRTPDepay_IgnoresPartialParameterSet.
func TestRepairAVCC_IgnoresPartialParameterSet(t *testing.T) {
	pps := avccNAL(NALUTypePPS, 0x44)
	iframe := avccNAL(NALUTypeIFrame, 0xAA, 0xBB, 0xCC, 0xDD)

	codec := &core.Codec{Name: core.CodecH264, FmtpLine: ""}

	var got []byte
	fn := RepairAVCC(codec, func(packet *rtp.Packet) {
		got = append([]byte(nil), packet.Payload...)
	})

	send := func(nal []byte) {
		fn(&rtp.Packet{Payload: append([]byte(nil), nal...)})
	}

	send(pps)
	send(iframe)
	require.Equal(t, iframe, got, "must not inject a PPS with no matching SPS")
}
