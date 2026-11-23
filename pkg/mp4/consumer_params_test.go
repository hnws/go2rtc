package mp4

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/pion/rtp"
	"github.com/stretchr/testify/require"
)

var (
	pSPS, _ = hex.DecodeString("67641028acb80f0044fcb80880000003008000000a02")
	pPPS, _ = hex.DecodeString("68ee0f2c8b")
	pIDR    = []byte{0x65, 0x88, 0x84, 0x04, 0xBF, 0xFE, 0xF7, 0xAD, 0xDF, 0x81}
	dummy   = []byte{0x67, 0x42, 0x00, 0x0a, 0xf8, 0x41, 0xa2} // muxer's placeholder SPS
)

func avcc(nalus ...[]byte) []byte {
	var b []byte
	for _, n := range nalus {
		b = binary.BigEndian.AppendUint32(b, uint32(len(n)))
		b = append(b, n...)
	}
	return b
}

// firstWrite captures the first Write (the init segment) and then blocks the
// rest, like a client still reading.
type firstWrite struct{ ch chan []byte }

func (w *firstWrite) Write(p []byte) (int, error) {
	select {
	case w.ch <- append([]byte(nil), p...):
	default:
	}
	return len(p), nil
}

func startConsumer(t *testing.T, src *core.Codec) (*Consumer, *core.Receiver, chan []byte) {
	t.Helper()
	cons := NewConsumer(nil)
	media := cons.Medias[0]
	rx := core.NewReceiver(&core.Media{Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{src}}, src)
	require.NoError(t, cons.AddTrack(media, media.Codecs[0], rx))
	w := &firstWrite{ch: make(chan []byte, 1)}
	go func() { _, _ = cons.WriteTo(w) }()
	t.Cleanup(func() { _ = cons.Stop() })
	return cons, rx, w.ch
}

func waitInit(t *testing.T, ch chan []byte, within time.Duration) []byte {
	t.Helper()
	select {
	case b := <-ch:
		return b
	case <-time.After(within):
		t.Fatal("no init segment")
		return nil
	}
}

func rawH264(fmtp string) *core.Codec {
	return &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: core.PayloadTypeRAW, FmtpLine: fmtp}
}

// Source never announced SPS/PPS: the init segment waits for the first
// keyframe and carries its real SPS, not the dummy.
func TestInitWaitsForKeyframeParams(t *testing.T) {
	_, rx, ch := startConsumer(t, rawH264("packetization-mode=1;profile-level-id=64001f"))

	select {
	case <-ch:
		t.Fatal("init written before any keyframe")
	case <-time.After(200 * time.Millisecond):
	}

	rx.WriteRTP(&rtp.Packet{Header: rtp.Header{Timestamp: 1000}, Payload: avcc(pSPS, pPPS, pIDR)})
	init := waitInit(t, ch, 2*time.Second)
	require.True(t, bytes.Contains(init, pSPS), "real SPS in avcC")
	require.True(t, bytes.Contains(init, pPPS), "real PPS in avcC")
	require.False(t, bytes.Contains(init, dummy), "no placeholder SPS")
}

// Parameter sets already learned by the producer: no wait at all.
func TestInitUsesLearnedParams(t *testing.T) {
	src := rawH264("packetization-mode=1")
	h264.LearnRTP(src, pSPS)
	h264.LearnRTP(src, pPPS)
	defer h264.ForgetParams(src)

	_, _, ch := startConsumer(t, src)
	init := waitInit(t, ch, 500*time.Millisecond)
	require.True(t, bytes.Contains(init, pSPS))
	require.False(t, bytes.Contains(init, dummy))
}

// No keyframe ever arrives: fall back to the old behaviour after paramsWait.
func TestInitWaitIsBounded(t *testing.T) {
	old := paramsWait
	paramsWait = 300 * time.Millisecond
	defer func() { paramsWait = old }()

	_, _, ch := startConsumer(t, rawH264("packetization-mode=1"))
	init := waitInit(t, ch, 2*time.Second)
	require.True(t, bytes.Contains(init, dummy), "placeholder as before")
}

// Announced parameter sets: unchanged behaviour, no wait.
func TestInitAnnouncedParamsUnchanged(t *testing.T) {
	_, _, ch := startConsumer(t, rawH264("packetization-mode=1;sprop-parameter-sets=Z2QQKKy4DwBE/LgIgAAAAwCAAAAKAg==,aO4PLIs="))
	init := waitInit(t, ch, 500*time.Millisecond)
	require.True(t, bytes.Contains(init, pSPS))
}
