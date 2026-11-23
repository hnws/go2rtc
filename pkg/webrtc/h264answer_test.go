package webrtc

import (
	"encoding/base64"
	"strings"
	"testing"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264"
	"github.com/AlexxIT/go2rtc/pkg/mp4"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	"github.com/stretchr/testify/require"
)

// SPS/PPS prefixes as cameras send them (only the first bytes matter here):
// 4K Dahua Main (4D 00 29), Nest High (64 00 1F), a Baseline camera (42 E0 1F).
var (
	spsMain     = []byte{0x67, 0x4D, 0x00, 0x29, 0x8D, 0x8D, 0x40, 0x3C, 0x01, 0x13, 0xF2, 0xCD}
	spsHigh     = []byte{0x67, 0x64, 0x00, 0x1F, 0xAC, 0x2C, 0xA4, 0x02, 0x80, 0x2D, 0xD8}
	spsBaseline = []byte{0x67, 0x42, 0xE0, 0x1F, 0xDA, 0x01, 0x40, 0x16, 0xEC}
	ppsAny      = []byte{0x68, 0xEE, 0x3C, 0x80}
)

func srcCodec(sps []byte) *core.Codec {
	return &core.Codec{
		Name: core.CodecH264, ClockRate: 90000, PayloadType: 96,
		FmtpLine: "packetization-mode=1;sprop-parameter-sets=" +
			base64.StdEncoding.EncodeToString(sps) + "," + base64.StdEncoding.EncodeToString(ppsAny),
	}
}

// the H.264 list a stock go2rtc client offers (pkg/webrtc/api.go)
func offeredVideo() *core.Media {
	return &core.Media{Kind: core.KindVideo, Direction: core.DirectionSendonly, Codecs: []*core.Codec{
		{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96, FmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f"},
		{Name: core.CodecH264, ClockRate: 90000, PayloadType: 97, FmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f"},
		{Name: core.CodecH264, ClockRate: 90000, PayloadType: 98, FmtpLine: "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640032"},
	}}
}

func TestChooseH264(t *testing.T) {
	m := offeredVideo()
	first := m.Codecs[0]

	require.Equal(t, uint8(98), chooseH264(m, first, srcCodec(spsHigh)).PayloadType, "High source -> High entry")
	require.Equal(t, uint8(98), chooseH264(m, first, srcCodec(spsMain)).PayloadType, "Main source -> High entry (superset)")
	require.Equal(t, uint8(96), chooseH264(m, first, srcCodec(spsBaseline)).PayloadType, "Baseline source -> first Baseline entry")

	// no parameter sets and no profile-level-id: keep what the stream matched
	unknown := &core.Codec{Name: core.CodecH264, ClockRate: 90000, FmtpLine: "packetization-mode=1"}
	require.Same(t, first, chooseH264(m, first, unknown))

	// profile-level-id alone is enough to pick the family
	plid := &core.Codec{Name: core.CodecH264, ClockRate: 90000, FmtpLine: "packetization-mode=1;profile-level-id=64001f"}
	require.Equal(t, uint8(98), chooseH264(m, first, plid).PayloadType)

	// a browser offering only Baseline: nothing better, keep the matched entry
	baselineOnly := &core.Media{Kind: core.KindVideo, Codecs: m.Codecs[:2]}
	require.Same(t, first, chooseH264(baselineOnly, first, srcCodec(spsMain)))

	// packetization-mode must match the entry the stream matched
	pm0 := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 99, FmtpLine: "packetization-mode=0;profile-level-id=640032"}
	mixed := &core.Media{Kind: core.KindVideo, Codecs: []*core.Codec{first, pm0}}
	require.Same(t, first, chooseH264(mixed, first, srcCodec(spsMain)))
}

func TestAnswerH264Codec(t *testing.T) {
	m := offeredVideo()
	p := answerH264Codec(m.Codecs[2], srcCodec(spsMain))
	require.Equal(t, webrtc.PayloadType(98), p.PayloadType)
	require.True(t, strings.HasPrefix(p.SDPFmtpLine, m.Codecs[2].FmtpLine+";sprop-parameter-sets="))
	require.NotEmpty(t, p.RTCPFeedback)

	// no parameter sets known: fmtp unchanged
	p = answerH264Codec(m.Codecs[0], &core.Codec{Name: core.CodecH264, FmtpLine: "packetization-mode=1"})
	require.Equal(t, m.Codecs[0].FmtpLine, p.SDPFmtpLine)
}

// End to end: a client built like a stock go2rtc (same codec list) offers to
// receive video, this Conn answers for a source with real parameter sets, and
// the client computes the MSE label from the answer the way go2rtc's MSE does.
func TestAnswerCarriesSourceProfile(t *testing.T) {
	for _, tc := range []struct {
		name  string
		sps   []byte
		label string
	}{
		{"4K Main camera", spsMain, "avc1.4D0029"},
		{"Nest High", spsHigh, "avc1.64001F"},
		{"Baseline camera", spsBaseline, "avc1.42E01F"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, err := NewAPI()
			require.NoError(t, err)

			client, err := api.NewPeerConnection(webrtc.Configuration{})
			require.NoError(t, err)
			defer client.Close()
			_, err = client.AddTransceiverFromKind(webrtc.RTPCodecTypeVideo,
				webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
			require.NoError(t, err)
			offer, err := client.CreateOffer(nil)
			require.NoError(t, err)
			require.NoError(t, client.SetLocalDescription(offer))

			pc, err := api.NewPeerConnection(webrtc.Configuration{})
			require.NoError(t, err)
			defer pc.Close()
			conn := NewConn(pc)
			conn.Mode = core.ModePassiveConsumer
			require.NoError(t, conn.SetOffer(offer.SDP))

			// what the stream does: match the source against the consumer's
			// medias; the first offered H.264 entry is what it picks today
			var media *core.Media
			for _, m := range conn.GetMedias() {
				if m.Kind == core.KindVideo {
					media = m
				}
			}
			require.NotNil(t, media)
			src := srcCodec(tc.sps)
			track := core.NewReceiver(&core.Media{Kind: core.KindVideo, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{src}}, src)
			require.NoError(t, conn.AddTrack(media, media.Codecs[0], track))

			answer, err := conn.GetAnswer()
			require.NoError(t, err)

			sd := &sdp.SessionDescription{}
			require.NoError(t, sd.Unmarshal([]byte(answer)))
			var h264s []*core.Codec
			for _, m := range UnmarshalMedias(sd.MediaDescriptions) {
				for _, c := range m.Codecs {
					if c.Name == core.CodecH264 {
						h264s = append(h264s, c)
					}
				}
			}
			require.Len(t, h264s, 1, "answer narrowed to one H.264 entry")
			require.Contains(t, answer, "nack pli", "RTCP feedback kept")
			require.Equal(t, `video/mp4; codecs="`+tc.label+`"`, mp4.ContentType(h264s))

			// the client must accept the narrowed answer
			require.NoError(t, client.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}))
		})
	}
}

// A WebRTC-sourced codec (Nest) has no sprop-parameter-sets; after the
// producer learned them in-band, the answer carries them and a relaying
// go2rtc labels the stream from the real SPS.
func TestAnswerUsesLearnedParams(t *testing.T) {
	src := &core.Codec{Name: core.CodecH264, ClockRate: 90000, PayloadType: 96, FmtpLine: "packetization-mode=1;profile-level-id=42e01f"}
	sps, _ := base64.StdEncoding.DecodeString("Z2QQKKy4DwBE/LgIgAAAAwCAAAAKAg==")
	h264.LearnRTP(src, sps)
	h264.LearnRTP(src, ppsAny)
	defer h264.ForgetParams(src)

	m := offeredVideo()
	chosen := chooseH264(m, m.Codecs[0], src)
	require.Equal(t, uint8(98), chosen.PayloadType, "learned SPS says High")
	p := answerH264Codec(chosen, src)
	require.Contains(t, p.SDPFmtpLine, "sprop-parameter-sets=Z2QQKKy4DwBE/LgIgAAAAwCAAAAKAg==,")
	require.Equal(t, `video/mp4; codecs="avc1.640028"`, mp4.ContentType([]*core.Codec{{Name: core.CodecH264, FmtpLine: p.SDPFmtpLine}}))
}
