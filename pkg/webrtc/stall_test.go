package webrtc

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	"github.com/rs/zerolog"
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
	pli       atomic.Int64 // PictureLossIndication packets received for video
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
	return connectProducer(t, "")
}

func connectProducer(t *testing.T, formatName string) (*Conn, *fakeCamera) {
	t.Helper()
	return connectProducerOpts(t, formatName, false)
}

// connectProducerOpts is connectProducer with the camera's video optionally
// off from the very start, so the producer never gets a single video packet.
func connectProducerOpts(t *testing.T, formatName string, videoOff bool) (*Conn, *fakeCamera) {
	t.Helper()

	pcProd, err := loopbackAPI(t).NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	prod := NewConn(pcProd)
	prod.Mode = core.ModeActiveProducer
	prod.FormatName = formatName

	medias := []*core.Media{
		{Kind: core.KindAudio, Direction: core.DirectionRecvonly},
		{Kind: core.KindVideo, Direction: core.DirectionRecvonly},
	}
	offer, err := prod.CreateCompleteOffer(medias)
	require.NoError(t, err)

	pcCam, err := loopbackAPI(t).NewPeerConnection(webrtc.Configuration{})
	require.NoError(t, err)
	cam := &fakeCamera{pc: pcCam, stop: make(chan struct{})}
	cam.videoOff.Store(videoOff)

	cam.audio, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48000, Channels: 2}, "audio", "cam")
	require.NoError(t, err)
	cam.video, err = webrtc.NewTrackLocalStaticRTP(webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeH264, ClockRate: 90000, SDPFmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"}, "video", "cam")
	require.NoError(t, err)

	require.NoError(t, pcCam.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}))
	// AddTrack after SetRemoteDescription reuses the offer's transceivers
	_, err = pcCam.AddTrack(cam.audio)
	require.NoError(t, err)
	videoSender, err := pcCam.AddTrack(cam.video)
	require.NoError(t, err)
	go func() { // count keyframe requests (PLI) the producer sends for video
		for {
			pkts, _, err := videoSender.ReadRTCP()
			if err != nil {
				return
			}
			for _, pkt := range pkts {
				if _, ok := pkt.(*rtcp.PictureLossIndication); ok {
					cam.pli.Add(1)
				}
			}
		}
	}()
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
		return prod.pc.ConnectionState() == webrtc.PeerConnectionStateConnected && cam.videoSent.Load() > 10
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

// captureLogs points the package Logger at a buffer for the test.
func captureLogs(t *testing.T) *syncBuffer {
	buf := &syncBuffer{}
	old := Logger
	Logger = zerolog.New(buf)
	t.Cleanup(func() { Logger = old })
	return buf
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// An ordinary teardown makes the blocked reads return io.EOF (or an
// interceptor-closed error). That must not be reported or handled as a
// failure - neither for our own Close nor for the streams-side Stop, which
// goes through the promoted core.Connection.Stop and closes pion directly.
func TestTrackRead_TeardownIsNotReportedAsFailure(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func(*Conn) error
	}{
		{"Close", func(c *Conn) error { return c.Close() }},
		{"streams-side Stop", func(c *Conn) error { return c.Stop() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withStallTimeout(t, 2*time.Second)
			logs := captureLogs(t)
			prod, cam := connectActiveProducer(t)
			waitConnected(t, prod, cam)

			done := startDone(prod)
			require.NoError(t, tc.stop(prod))
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("Start did not return after the connection was stopped")
			}
			time.Sleep(300 * time.Millisecond) // let both read loops observe the teardown
			require.NotContains(t, logs.String(), "read failed on a live connection")
			require.NotContains(t, logs.String(), "stalled")
		})
	}
}

// The stall path logs exactly one clear reason, and not a generic read failure.
func TestVideoStall_LogsTheStall(t *testing.T) {
	withStallTimeout(t, time.Second)
	logs := captureLogs(t)
	prod, cam := connectActiveProducer(t)
	waitConnected(t, prod, cam)

	done := startDone(prod)
	cam.videoOff.Store(true)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("stall not detected")
	}
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, 1, strings.Count(logs.String(), "video track stalled"))
	require.NotContains(t, logs.String(), "read failed on a live connection")
}

// Nest sources get a keyframe request (PLI) every 2s so consumers that join
// mid-stream don't wait for the camera's own long keyframe interval.
func TestKeyframeRequest_SentForNest(t *testing.T) {
	prod, cam := connectProducer(t, "nest/webrtc")
	waitConnected(t, prod, cam)

	require.Eventually(t, func() bool { return cam.pli.Load() >= 2 }, 7*time.Second, 50*time.Millisecond,
		"expected periodic PLIs for a nest/webrtc source, got %d", cam.pli.Load())
}

// Other active WebRTC sources (e.g. battery cameras) must not be forced into
// frequent keyframes.
func TestKeyframeRequest_NotSentForOtherActiveProducers(t *testing.T) {
	prod, cam := connectProducer(t, "webrtc/whep")
	waitConnected(t, prod, cam)

	time.Sleep(5 * time.Second)
	require.Zero(t, cam.pli.Load(), "a non-Nest active producer received PLIs")
}

// A session that connects but never sends a single video packet: pion never
// fires OnTrack for the video, so the per-track stall deadline is never armed.
// The first-video watchdog must close it with core.ErrNoMedia (the streams
// layer then backs off). Seen on a Nest doorbell 2026-11-09, stuck for hours.
func TestFirstVideo_ClosesWhenNoVideoEverArrives(t *testing.T) {
	withStallTimeout(t, time.Second)
	logs := captureLogs(t)
	prod, cam := connectProducerOpts(t, "nest/webrtc", true)
	require.Eventually(t, func() bool {
		return prod.pc.ConnectionState() == webrtc.PeerConnectionStateConnected
	}, 10*time.Second, 20*time.Millisecond, "peers never connected")
	require.Zero(t, cam.videoSent.Load())

	errc := make(chan error, 1)
	started := time.Now()
	go func() { errc <- prod.Start() }()

	select {
	case err := <-errc:
		require.ErrorIs(t, err, core.ErrNoMedia)
		require.GreaterOrEqual(t, time.Since(started), 900*time.Millisecond, "closed before the timeout")
	case <-time.After(5 * time.Second):
		t.Fatal("connection without any video stayed open")
	}
	require.Equal(t, 1, strings.Count(logs.String(), "no video received since start"))
}

// Video that does arrive keeps the connection open past the first-video
// timeout, and an ordinary close afterwards still returns nil.
func TestFirstVideo_KeepsStreamWithVideoOpen(t *testing.T) {
	withStallTimeout(t, time.Second)
	prod, cam := connectProducer(t, "nest/webrtc")
	waitConnected(t, prod, cam)

	errc := make(chan error, 1)
	go func() { errc <- prod.Start() }()
	select {
	case err := <-errc:
		t.Fatalf("a connection with flowing video was closed: %v", err)
	case <-time.After(3 * time.Second):
	}

	require.NoError(t, prod.Close())
	select {
	case err := <-errc:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after Close")
	}
}
