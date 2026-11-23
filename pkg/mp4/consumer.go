package mp4

import (
	"errors"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/h265"
	"github.com/AlexxIT/go2rtc/pkg/pcm"
	"github.com/pion/rtp"
)

type Consumer struct {
	core.Connection
	wr    *core.WriteBuffer
	muxer *Muxer
	mu    sync.Mutex
	start bool

	// An H.264 source that never announced SPS/PPS (WebRTC: Nest) would get a
	// dummy SPS in the init segment, which Firefox's decoder can't handle.
	// WriteTo then waits for the first keyframe and builds the init segment
	// from its in-band SPS/PPS (paramsReady), up to paramsWait.
	paramsReady chan struct{}
	paramsOnce  sync.Once

	Rotate int `json:"-"`
	ScaleX int `json:"-"`
	ScaleY int `json:"-"`
}

// paramsWait caps how long WriteTo waits for a first keyframe with SPS/PPS.
var paramsWait = 5 * time.Second

func NewConsumer(medias []*core.Media) *Consumer {
	if medias == nil {
		// default local medias
		medias = []*core.Media{
			{
				Kind:      core.KindVideo,
				Direction: core.DirectionSendonly,
				Codecs: []*core.Codec{
					{Name: core.CodecH264},
					{Name: core.CodecH265},
				},
			},
			{
				Kind:      core.KindAudio,
				Direction: core.DirectionSendonly,
				Codecs: []*core.Codec{
					{Name: core.CodecAAC},
				},
			},
		}
	}

	wr := core.NewWriteBuffer(nil)
	return &Consumer{
		Connection: core.Connection{
			ID:         core.NewID(),
			FormatName: "mp4",
			Medias:     medias,
			Transport:  wr,
		},
		muxer: &Muxer{},
		wr:    wr,
	}
}

func (c *Consumer) AddTrack(media *core.Media, _ *core.Codec, track *core.Receiver) error {
	trackID := byte(len(c.Senders))
	var learnedFmtp string

	codec := track.Codec.Clone()
	handler := core.NewSender(media, codec)

	switch track.Codec.Name {
	case core.CodecH264:
		// use what the producer learned in-band when the source's SDP had no
		// parameter sets; otherwise wait for this stream's first keyframe
		needParams := false
		if fmtp := h264.LearnedFmtpLine(track.Codec); strings.Contains(fmtp, "sprop-parameter-sets=") {
			learnedFmtp = fmtp // applied once the muxer has this track
		} else if c.paramsReady == nil {
			c.paramsReady = make(chan struct{})
			needParams = true
		}

		handler.Handler = func(packet *rtp.Packet) {
			if !c.start {
				if !h264.IsKeyframe(packet.Payload) {
					return
				}
				c.start = true
			}

			// important to use Mutex because right fragment order
			c.mu.Lock()
			// a keyframe may arrive without its SPS/PPS (they were sent before
			// this consumer joined); keep waiting for one that has them
			if needParams && h264.IsKeyframe(packet.Payload) {
				if fmtp := h264.GetFmtpLine(packet.Payload); strings.Contains(fmtp, "sprop-parameter-sets=") &&
					strings.Contains(core.Between(fmtp, "sprop-parameter-sets=", ";"), ",") {
					needParams = false
					c.muxer.SetFmtp(int(trackID), fmtp)
					c.paramsOnce.Do(func() { close(c.paramsReady) })
				}
			}
			b := c.muxer.GetPayload(trackID, packet)
			if n, err := c.wr.Write(b); err == nil {
				c.Send += n
			}
			c.mu.Unlock()
		}

		if track.Codec.IsRTP() {
			handler.Handler = h264.RTPDepay(track.Codec, handler.Handler)
		} else {
			handler.Handler = h264.RepairAVCC(track.Codec, handler.Handler)
		}

	case core.CodecH265:
		handler.Handler = func(packet *rtp.Packet) {
			if !c.start {
				if !h265.IsKeyframe(packet.Payload) {
					return
				}
				c.start = true
			}

			// important to use Mutex because right fragment order
			c.mu.Lock()
			b := c.muxer.GetPayload(trackID, packet)
			if n, err := c.wr.Write(b); err == nil {
				c.Send += n
			}
			c.mu.Unlock()
		}

		if track.Codec.IsRTP() {
			handler.Handler = h265.RTPDepay(track.Codec, handler.Handler)
		} else {
			handler.Handler = h265.RepairAVCC(track.Codec, handler.Handler)
		}

	default:
		handler.Handler = func(packet *rtp.Packet) {
			if !c.start {
				return
			}

			// important to use Mutex because right fragment order
			c.mu.Lock()
			b := c.muxer.GetPayload(trackID, packet)
			if n, err := c.wr.Write(b); err == nil {
				c.Send += n
			}
			c.mu.Unlock()
		}

		switch track.Codec.Name {
		case core.CodecAAC:
			if track.Codec.IsRTP() {
				handler.Handler = aac.RTPDepay(handler.Handler)
			}
		case core.CodecOpus, core.CodecMP3: // no changes
		case core.CodecPCMA, core.CodecPCMU, core.CodecPCM, core.CodecPCML:
			codec.Name = core.CodecFLAC
			if codec.Channels == 2 {
				// hacky way for support two channels audio
				codec.Channels = 1
				codec.ClockRate *= 2
			}
			handler.Handler = pcm.FLACEncoder(track.Codec.Name, codec.ClockRate, handler.Handler)

		default:
			handler.Handler = nil
		}
	}

	if handler.Handler == nil {
		s := "mp4: unsupported codec: " + track.Codec.String()
		println(s)
		return errors.New(s)
	}

	c.muxer.AddTrack(codec)
	if learnedFmtp != "" {
		c.muxer.SetFmtp(int(trackID), learnedFmtp)
	}

	handler.HandleRTP(track)
	c.Senders = append(c.Senders, handler)

	return nil
}

func (c *Consumer) WriteTo(wr io.Writer) (int64, error) {
	if len(c.Senders) == 1 && c.Senders[0].Codec.IsAudio() {
		c.start = true
	}

	if c.paramsReady != nil {
		select {
		case <-c.paramsReady:
		case <-time.After(paramsWait):
		}
	}

	c.mu.Lock()
	init, err := c.muxer.GetInit()
	c.mu.Unlock()
	if err != nil {
		return 0, err
	}

	if c.Rotate != 0 {
		PatchVideoRotate(init, c.Rotate)
	}
	if c.ScaleX != 0 && c.ScaleY != 0 {
		PatchVideoScale(init, c.ScaleX, c.ScaleY)
	}

	if _, err = wr.Write(init); err != nil {
		return 0, err
	}

	return c.wr.WriteTo(wr)
}
