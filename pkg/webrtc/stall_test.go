package webrtc

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// fakeCamera is a remote peer that sends H264 video + Opus audio, standing in
// for a Nest camera. Video can be switched off independently of audio.
type fakeCamera struct {
	pc        *webrtc.PeerConnection
	video     *webrtc.TrackLocalStaticRTP
	audio     *webrtc.TrackLocalStaticRTP
	videoOff  atomic.Bool
	stop      chan struct{}
	videoSent atomic.Int64
}

func loopbackAPI(t *testing.T) *webrtc.API {
	api, err := NewServerAPI("", "", &Filters{Loopback: true, Interfaces: []string{"lo"}, Networks: []string{"udp4"}})
	require.NoError(t, err)
	return api
}

// connectActiveProducer wires a go2rtc Conn (as an active producer, like the
// Nest client) to a fakeCamera over loopback and starts the camera sending.
func connectActiveProducer(t *testing.T) (*Conn, *fakeCamera) {
	t.Helper()

	pcProd, err := loopbackAPI(t).NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	prod := NewConn(pcProd)
	prod.Mode = core.ModeActiveProducer

	medias := []*core.Media{
		{Kind: core.KindAudio, Direction: core.DirectionRecvonly},
		{Kind: core.KindVideo, Direction: core.DirectionRecvonly},
	}
	offer, err := prod.CreateCompleteOffer(medias)
	require.NoError(t, err)

	pcCam, err := loopbackAPI(t).NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	cam := &fakeCamera{pc: pcCam, stop: make(chan struct{})}

	cam.audio, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "cam")
	require.NoError(t, err)
	cam.video, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"}, "video", "cam")
	require.NoError(t, err)

	require.NoError(t, pcCam.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}))
	// AddTrack after SetRemoteDescription reuses the offer's transceivers
	_, err = pcCam.AddTrack(cam.audio)
	require.NoError(t, err)
	_, err = pcCam.AddTrack(cam.video)
	require.NoError(t, err)
	answer, err := pcCam.CreateAnswer(nil)
	require.NoError(t, err)
	gathered := webrtc.GatheringCompletePromise(pcCam)
	require.NoError(t, pcCam.SetLocalDescription(answer))
	<-gathered
	require.NoError(t, prod.SetAnswer(pcCam.LocalDescription().SDP))

	go cam.send()
	t.Cleanup(func() {
		close(cam.stop)
		_ = pcCam.Close()
		_ = prod.Close()
	})
	return prod, cam
}

func (c *fakeCamera) send() {
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	var seq uint16
	for {
		select {
		case <-c.stop:
			return
		case <-tick.C:
		}
		seq++
		ts := uint32(seq) * 960
		_ = c.audio.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: seq, Timestamp: ts}, Payload: []byte{0xf8, 0xff, 0xfe}})
		if !c.videoOff.Load() {
			// a tiny single-NAL "frame" is enough: only packet arrival matters here
			if c.video.WriteRTP(&rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96, SequenceNumber: seq, Timestamp: uint32(seq) * 1800, Marker: true}, Payload: []byte{0x65, 0x88, 0x84}}) == nil {
				c.videoSent.Add(1)
			}
		}
	}
}

// startDone runs prod.Start() (which blocks until the connection closes) and
// returns a channel closed when it returns - i.e. when the Producer would
// arm its reconnect.
func startDone(prod *Conn) chan struct{} {
	done := make(chan struct{})
	go func() {
		_ = prod.Start()
		close(done)
	}()
	return done
}

func withStallTimeout(t *testing.T, d time.Duration) {
	old := VideoStallTimeout
	VideoStallTimeout = d
	t.Cleanup(func() { VideoStallTimeout = old })
}

func waitConnected(t *testing.T, prod *Conn, cam *fakeCamera) {
	t.Helper()
	require.Eventually(t, func() bool {
		return prod.pc.ConnectionState() == webrtc.PeerConnectionStateConnected && cam.videoSent.Load() > 10 && prod.Recv > 0
	}, 10*time.Second, 20*time.Millisecond, "peers never connected / no media")
}

// The Nest failure: video stops with no error while audio keeps flowing and
// the PeerConnection stays connected. The watchdog must close the connection.
func TestVideoStall_ClosesConnectionWhenOnlyAudioKeepsFlowing(t *testing.T) {
	withStallTimeout(t, 2*time.Second)
	prod, cam := connectActiveProducer(t)
	waitConnected(t, prod, cam)

	done := startDone(prod)
	cam.videoOff.Store(true)
	stoppedAt := time.Now()

	select {
	case <-done:
		// closes after roughly the timeout (up to 1/4 of it early, per re-arm granularity), not instantly
		require.GreaterOrEqual(t, time.Since(stoppedAt), 1500*time.Millisecond)
	case <-time.After(6 * time.Second):
		t.Fatal("connection stayed open although video stalled while audio kept flowing")
	}
}

// Healthy streams must never be closed by the watchdog.
func TestVideoStall_KeepsHealthyStreamOpen(t *testing.T) {
	withStallTimeout(t, time.Second)
	prod, cam := connectActiveProducer(t)
	waitConnected(t, prod, cam)

	done := startDone(prod)
	select {
	case <-done:
		t.Fatal("watchdog closed a connection whose video was still flowing")
	case <-time.After(4 * time.Second):
	}
}
