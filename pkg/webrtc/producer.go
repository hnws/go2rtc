package webrtc

import (
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/webrtc/v4"
)

func (c *Conn) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	core.Assert(media.Direction == core.DirectionRecvonly)

	// Match by media, not by codec pointer. pion's OnTrack can fire more than
	// once for the same logical m-line within the life of a single
	// PeerConnection (e.g. the remote renegotiates and hands us a fresh
	// TrackRemote for the same mid), and getMediaCodec re-parses a brand new
	// *core.Codec each time even when the content is identical. Matching on
	// codec pointer identity would miss that and allocate a second, orphaned
	// Receiver that nothing ever feeds - existing consumers stay bound to the
	// original Receiver while any new consumer that attaches afterward gets
	// wired to the dead one instead, silently going to zero bytes forever.
	for _, track := range c.Receivers {
		if track.Media == media {
			track.Codec = codec
			return track, nil
		}
	}

	switch c.Mode {
	case core.ModePassiveConsumer: // backchannel from browser
		// set codec for consumer recv track so remote peer should send media with this codec
		params := webrtc.RTPCodecParameters{
			RTPCodecCapability: webrtc.RTPCodecCapability{
				MimeType:  MimeType(codec),
				ClockRate: codec.ClockRate,
				Channels:  uint16(codec.Channels),
			},
			PayloadType: 0, // don't know if this necessary
		}

		tr := c.getTranseiver(media.ID)

		_ = tr.SetCodecPreferences([]webrtc.RTPCodecParameters{params})

	case core.ModePassiveProducer, core.ModeActiveProducer:
		// Passive producers: OBS Studio via WHIP or Browser
		// Active producers: go2rtc as WebRTC client or WebTorrent

	default:
		panic(core.Caller())
	}

	track := core.NewReceiver(media, codec)
	c.Receivers = append(c.Receivers, track)
	return track, nil
}

func (c *Conn) Start() error {
	c.closed.Wait()
	return nil
}
