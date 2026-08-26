package streams

import (
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/stretchr/testify/require"
)

type fakeTrackConn struct {
	receivers []*core.Receiver
}

func (f *fakeTrackConn) GetMedias() []*core.Media { return nil }

func (f *fakeTrackConn) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	track := core.NewReceiver(media, codec)
	f.receivers = append(f.receivers, track)
	return track, nil
}

func (f *fakeTrackConn) Start() error { return nil }
func (f *fakeTrackConn) Stop() error  { return nil }

// TestProducerGetTrack_RenegotiationReusesReceiver mirrors the same bug
// fixed in pkg/webrtc.Conn.GetTrack, one layer up: every new consumer
// negotiates and parses its own fresh *core.Codec, so a cache keyed on codec
// pointer identity here missed on almost every single consumer attach and
// appended a fresh, redundant entry into p.receivers every time - even
// though the underlying conn.GetTrack already correctly returned the same
// track. Left running for a stream an NVR reconnects to every few seconds,
// that list grows unbounded and makes every future attach/detach scan of it
// progressively slower.
func TestProducerGetTrack_RenegotiationReusesReceiver(t *testing.T) {
	conn := &fakeTrackConn{}
	p := &Producer{conn: conn, state: stateMedias}

	media := &core.Media{Kind: core.KindVideo, Direction: core.DirectionRecvonly}

	codec1 := &core.Codec{Name: core.CodecH264, PayloadType: 96}
	track1, err := p.GetTrack(media, codec1)
	require.NoError(t, err)
	require.Len(t, p.receivers, 1)
	require.Len(t, conn.receivers, 1)

	// simulate 50 more consumer attaches, each with its own freshly parsed
	// codec object for the exact same media, as happens on every real
	// consumer negotiation
	for i := 0; i < 50; i++ {
		codec := &core.Codec{Name: core.CodecH264, PayloadType: 96}
		track, err := p.GetTrack(media, codec)
		require.NoError(t, err)
		require.Same(t, track1, track, "must keep returning the same Receiver for the same media")
	}

	require.Len(t, p.receivers, 1, "must not accumulate a new entry per consumer attach")
	require.Len(t, conn.receivers, 1, "must not call the underlying conn.GetTrack more than once for the same media")
}
