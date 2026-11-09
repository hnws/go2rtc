package webrtc

import (
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/webrtc/v4"
)

func (c *Conn) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	core.Assert(media.Direction == core.DirectionRecvonly)

	// OnTrack runs one goroutine per remote track, and each calls GetTrack
	// (the streams layer does too, e.g. on reconnect). Unsynchronized, two
	// concurrent calls could both miss in the lookup below and append at
	// once, losing a Receiver. Found with -race.
	c.tracksMu.Lock()
	defer c.tracksMu.Unlock()

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
	if t := c.armFirstVideoWatchdog(); t != nil {
		defer t.Stop()
	}
	// nil after an ordinary close; core.ErrNoMedia from the first-video
	// watchdog, so the streams layer backs off instead of retrying at once.
	return c.closed.Wait()
}

// Stop shadows the promoted core.Connection.Stop, which is what the streams
// layer (and nest.WebRTCClient.Stop) calls. It snapshots Receivers under
// tracksMu because a late OnTrack may still be appending in GetTrack while the
// stream is being stopped (found with -race). The closes run outside the lock
// so pc.Close can never wait on a track goroutine blocked in GetTrack.
func (c *Conn) Stop() error {
	c.tracksMu.Lock()
	receivers := append([]*core.Receiver(nil), c.Receivers...)
	c.tracksMu.Unlock()

	for _, receiver := range receivers {
		receiver.Close()
	}
	for _, sender := range c.Senders {
		sender.Close()
	}
	return c.pc.Close()
}
