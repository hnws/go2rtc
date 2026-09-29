package webrtc

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
)

var Logger = zerolog.Nop()

// VideoStallTimeout is how long an active producer's video track may go
// without a single RTP packet before the connection is treated as dead and
// closed, so the owning Producer reconnects. Zero disables the watchdog.
//
// Needed because a remote can stop sending video without any error: pion's
// Read just blocks forever while the PeerConnection stays "connected" and
// the other tracks keep flowing. Observed live on a Nest camera - video
// stopped at 05:29 while audio kept arriving and the Nest session kept
// being extended successfully, so nothing ever reconnected.
var VideoStallTimeout = 20 * time.Second

type Conn struct {
	core.Connection
	core.Listener

	Mode core.Mode `json:"mode"`

	pc *webrtc.PeerConnection

	offer  string
	closed core.Waiter

	recvMu   sync.Mutex // audio and video read loops both add to Recv
	tracksMu sync.Mutex // serializes GetTrack's lookup-or-create on Receivers
}

func NewConn(pc *webrtc.PeerConnection) *Conn {
	c := &Conn{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "webrtc",
			Transport:  pc,
		},
		pc: pc,
	}

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		// last candidate will be empty
		if candidate != nil {
			c.Fire(candidate)
		}
	})

	pc.OnDataChannel(func(channel *webrtc.DataChannel) {
		c.Fire(channel)
	})

	pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
		if state != webrtc.ICEConnectionStateChecking {
			return
		}
		pc.SCTP().Transport().ICETransport().OnSelectedCandidatePairChange(
			func(pair *webrtc.ICECandidatePair) {
				// fix situation when candidate pair changes multiple times
				if i := strings.IndexByte(c.Protocol, '+'); i > 0 {
					c.Protocol = c.Protocol[:i]
				}
				c.Protocol += "+" + pair.Remote.Protocol.String()
				c.RemoteAddr = fmt.Sprintf(
					"%s:%d %s", sanitizeIP6(pair.Remote.Address), pair.Remote.Port, pair.Remote.Typ,
				)
				if pair.Remote.RelatedAddress != "" {
					c.RemoteAddr += fmt.Sprintf(
						" %s:%d", sanitizeIP6(pair.Remote.RelatedAddress), pair.Remote.RelatedPort,
					)
				}
			},
		)
	})

	pc.OnTrack(func(remote *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		media, codec := c.getMediaCodec(remote)
		if media == nil {
			return
		}

		track, err := c.GetTrack(media, codec)
		if err != nil {
			return
		}

		switch c.Mode {
		case core.ModePassiveProducer, core.ModeActiveProducer:
			// replace the theoretical list of codecs with the actual list of codecs
			if len(media.Codecs) > 1 {
				media.Codecs = []*core.Codec{codec}
			}
		}

		if c.Mode == core.ModePassiveProducer && remote.Kind() == webrtc.RTPCodecTypeVideo {
			go func() {
				pkts := []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(remote.SSRC())}}
				for range time.NewTicker(time.Second * 2).C {
					if err := pc.WriteRTCP(pkts); err != nil {
						return
					}
				}
			}()
		}

		// Only for connections go2rtc opened itself (Nest, webrtc: sources):
		// those reconnect on close. Audio is excluded because Opus DTX can
		// legitimately send nothing during silence.
		stall := VideoStallTimeout
		watch := stall > 0 && c.Mode == core.ModeActiveProducer &&
			remote.Kind() == webrtc.RTPCodecTypeVideo
		var armed time.Time

		for {
			if watch {
				// Re-arm every quarter of the timeout rather than per packet.
				// The interval must stay well below the timeout itself, or a
				// healthy stream races its own deadline between re-arms.
				if now := time.Now(); now.Sub(armed) >= stall/4 {
					_ = remote.SetReadDeadline(now.Add(stall))
					armed = now
				}
			}

			b := make([]byte, ReceiveMTU)
			n, _, err := remote.Read(b)
			if err != nil {
				// Every teardown (our Close, the streams-side Stop via the
				// promoted core.Connection.Stop, a remote close) surfaces here
				// as a read error - io.EOF, or an unexported "interceptor is
				// closed" when transport-cc is negotiated. pion sets
				// SignalingStateClosed before it stops the receivers on all of
				// those paths, so that state, not the error value, tells an
				// ordinary shutdown from a real failure on a live connection.
				if pc.SignalingState() == webrtc.SignalingStateClosed {
					return
				}

				if watch && isTimeout(err) {
					Logger.Warn().Uint32("id", c.ID).Str("url", c.URL).
						Dur("timeout", stall).
						Msg("[webrtc] video track stalled, closing connection to reconnect")
				} else {
					Logger.Warn().Uint32("id", c.ID).Str("url", c.URL).
						Str("kind", remote.Kind().String()).Err(err).
						Msg("[webrtc] track read failed on a live connection, closing to reconnect")
				}
				// A read error on just this track (e.g. the remote drops the
				// SSRC on a mid-session renegotiation) used to end only this
				// goroutine silently - the PeerConnection stayed "connected"
				// with every other track still flowing, so nothing ever
				// noticed or reconnected. Observed live: a Nest producer's
				// audio track kept receiving for hours after its video track
				// died this way, and every consumer sat at zero video bytes
				// until the whole service was restarted by hand. Close the
				// connection so the owning Producer's normal reconnect path
				// (armReconnect/reconnect) picks it back up automatically.
				_ = c.Close()
				return
			}

			c.recvMu.Lock()
			c.Recv += n
			c.recvMu.Unlock()

			packet := &rtp.Packet{}
			if err := packet.Unmarshal(b[:n]); err != nil {
				continue // skip a single malformed packet, don't kill the whole track
			}

			if len(packet.Payload) == 0 {
				continue
			}

			track.WriteRTP(packet)
		}
	})

	// OK connection:
	// 15:01:46 ICE connection state changed: checking
	// 15:01:46 peer connection state changed: connected
	// 15:01:54 peer connection state changed: disconnected
	// 15:02:20 peer connection state changed: failed
	//
	// Fail connection:
	// 14:53:08 ICE connection state changed: checking
	// 14:53:39 peer connection state changed: failed
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		c.Fire(state)

		switch state {
		case webrtc.PeerConnectionStateConnected:
			for _, sender := range c.Senders {
				sender.Start()
			}
		case webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			// disconnect event comes earlier, than failed
			// but it comes only for success connections
			_ = c.Close()
		}
	})

	return c
}

// isTimeout reports whether err is a read-deadline expiry (pion returns a
// net.Error with Timeout() == true from its packetio buffers).
func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func (c *Conn) MarshalJSON() ([]byte, error) {
	return json.Marshal(c.Connection)
}

func (c *Conn) Close() error {
	c.closed.Done(nil)
	return c.pc.Close()
}

func (c *Conn) AddCandidate(candidate string) error {
	// pion uses only candidate value from json/object candidate struct
	return c.pc.AddICECandidate(webrtc.ICECandidateInit{Candidate: candidate})
}

func (c *Conn) GetSenderTrack(mid string) *Track {
	if tr := c.getTranseiver(mid); tr != nil {
		if s := tr.Sender(); s != nil {
			// Track() can be nil (or a different TrackLocal implementation)
			// during renegotiation - the single-value assertion form would
			// panic instead of just reporting no track.
			if t, ok := s.Track().(*Track); ok {
				return t
			}
		}
	}
	return nil
}

func (c *Conn) getTranseiver(mid string) *webrtc.RTPTransceiver {
	for _, tr := range c.pc.GetTransceivers() {
		if tr.Mid() == mid {
			return tr
		}
	}
	return nil
}

func (c *Conn) getMediaCodec(remote *webrtc.TrackRemote) (*core.Media, *core.Codec) {
	for _, tr := range c.pc.GetTransceivers() {
		// search Transeiver for this TrackRemote
		if tr.Receiver() == nil || tr.Receiver().Track() != remote {
			continue
		}

		// search Media for this MID
		for _, media := range c.Medias {
			if media.ID != tr.Mid() || media.Direction != core.DirectionRecvonly {
				continue
			}

			// search codec for this PayloadType
			for _, codec := range media.Codecs {
				if codec.PayloadType != uint8(remote.PayloadType()) {
					continue
				}
				return media, codec
			}
		}
	}

	// fix moment when core.ModePassiveProducer or core.ModeActiveProducer
	// sends new codec with new payload type to same media
	// check GetTrack
	panic(core.Caller())

	return nil, nil
}

func sanitizeIP6(host string) string {
	if strings.IndexByte(host, ':') > 0 {
		return "[" + host + "]"
	}
	return host
}
