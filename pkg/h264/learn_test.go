package h264

import (
	"encoding/hex"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

// SPS/PPS of a 1920x1080 High 4.0 stream (x264, one frame)
var (
	testSPS, _ = hex.DecodeString("67641028acb80f0044fcb80880000003008000000a02")
	testPPS, _ = hex.DecodeString("68ee0f2c8b")
)

func stapA(nalus ...[]byte) []byte {
	b := []byte{0x18} // STAP-A
	for _, n := range nalus {
		b = append(b, byte(len(n)>>8), byte(len(n)))
		b = append(b, n...)
	}
	return b
}

func TestLearnRTPSingleNALUs(t *testing.T) {
	c := &core.Codec{Name: core.CodecH264, ClockRate: 90000, FmtpLine: "packetization-mode=1"}
	defer ForgetParams(c)

	require.Equal(t, "packetization-mode=1", LearnedFmtpLine(c), "nothing learned yet")

	LearnRTP(c, []byte{0x65, 0x88, 0x84}) // IDR slice: ignored
	LearnRTP(c, testSPS)
	require.Equal(t, "packetization-mode=1", LearnedFmtpLine(c), "SPS alone is not enough")

	LearnRTP(c, testPPS)
	fmtp := LearnedFmtpLine(c)
	sps, pps := GetParameterSet(fmtp)
	require.Equal(t, testSPS, sps)
	require.Equal(t, testPPS, pps)
	require.Contains(t, fmtp, "profile-level-id=641028")
	require.Equal(t, "packetization-mode=1", c.FmtpLine, "the codec itself is never modified")

	s := DecodeSPS(sps)
	require.NotNil(t, s)
	require.Equal(t, uint16(1920), s.Width())
	require.Equal(t, uint16(1080), s.Height())
}

func TestLearnRTPSTAPA(t *testing.T) {
	c := &core.Codec{Name: core.CodecH264, ClockRate: 90000, FmtpLine: "packetization-mode=1;profile-level-id=64001f"}
	defer ForgetParams(c)

	LearnRTP(c, stapA(testSPS, testPPS))
	fmtp := LearnedFmtpLine(c)
	sps, pps := GetParameterSet(fmtp)
	require.Equal(t, testSPS, sps)
	require.Equal(t, testPPS, pps)
	require.Contains(t, fmtp, "profile-level-id=64001f", "existing profile-level-id kept")
	require.NotContains(t, fmtp, "profile-level-id=641028")

	// a newer SPS replaces the old one
	newer := append([]byte(nil), testSPS...)
	newer[len(newer)-1] ^= 0x01
	LearnRTP(c, newer)
	sps, _ = GetParameterSet(LearnedFmtpLine(c))
	require.Equal(t, newer, sps)

	// truncated STAP-A must not panic
	LearnRTP(c, []byte{0x18, 0x00, 0x40, 0x67})
}

func TestLearnedFmtpLineUntouched(t *testing.T) {
	withSprop := &core.Codec{Name: core.CodecH264, FmtpLine: "packetization-mode=1;sprop-parameter-sets=Z2QAKA==,aO4PLA=="}
	LearnRTP(withSprop, stapA(testSPS, testPPS))
	defer ForgetParams(withSprop)
	require.Equal(t, withSprop.FmtpLine, LearnedFmtpLine(withSprop), "announced parameter sets win")

	opus := &core.Codec{Name: core.CodecOpus, FmtpLine: "minptime=10"}
	require.Equal(t, "minptime=10", LearnedFmtpLine(opus))
	require.Equal(t, "", LearnedFmtpLine(nil))

	c := &core.Codec{Name: core.CodecH264, FmtpLine: "packetization-mode=1"}
	LearnRTP(c, stapA(testSPS, testPPS))
	ForgetParams(c)
	require.Equal(t, "packetization-mode=1", LearnedFmtpLine(c), "forgotten")
}
