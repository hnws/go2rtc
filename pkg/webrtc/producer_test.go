package webrtc

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

// TestGetTrack_RenegotiationReusesReceiver covers a duplicate-Receiver bug
// found live on production Nest streams: pion's OnTrack can fire more than
// once for the same logical m-line within one PeerConnection's life (e.g.
// the remote renegotiates and hands us a fresh TrackRemote for the same
// mid), and getMediaCodec re-parses a brand new *core.Codec each time even
// when its content is identical. GetTrack used to match by codec pointer
// identity, so the second call missed the cache and allocated a second,
// orphaned Receiver that OnTrack's read loop never feeds - any consumer
// that attached after the renegotiation got wired to the dead one and sat
// at zero bytes forever, while consumers attached before it kept working
// off the original, still-fed Receiver.
func TestGetTrack_RenegotiationReusesReceiver(t *testing.T) {
	c := &Conn{Mode: core.ModeActiveProducer}

	media := &core.Media{Kind: core.KindVideo, Direction: core.DirectionRecvonly}

	// first negotiation
	codec1 := &core.Codec{Name: core.CodecH264, PayloadType: 96}
	track1, err := c.GetTrack(media, codec1)
	require.Nil(t, err)
	require.Len(t, c.Receivers, 1)

	// remote renegotiates on the same mid: OnTrack fires again, produces a
	// brand new *core.Codec instance (same content is irrelevant - what
	// matters here is it's a different pointer, same as a fresh SDP parse
	// always allocates)
	codec2 := &core.Codec{Name: core.CodecH264, PayloadType: 96}
	track2, err := c.GetTrack(media, codec2)
	require.Nil(t, err)

	require.Same(t, track1, track2, "must reuse the same Receiver across a renegotiation on the same media, not allocate an orphan")
	require.Len(t, c.Receivers, 1, "must not accumulate a second, dead Receiver for the same media")
	require.Same(t, codec2, track1.Codec, "the reused Receiver must track the latest negotiated codec")
}
