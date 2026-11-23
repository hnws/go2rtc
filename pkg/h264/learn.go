package h264

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"strings"
	"sync"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

// Some sources never announce their H.264 parameter sets: a WebRTC SDP has no
// sprop-parameter-sets, so a Nest camera's codec only ever says
// "packetization-mode=1;profile-level-id=...". Everything go2rtc builds from
// that codec then lacks the real SPS/PPS: the MP4/MSE init segment falls back
// to a dummy 128x96 Baseline SPS (Firefox configures its decoder from it and
// fails), and a WebRTC answer gives a relaying go2rtc nothing to label with.
//
// The source does send SPS/PPS in-band with its keyframes. LearnRTP records
// the latest pair per source codec as packets arrive; LearnedFmtpLine returns
// the codec's fmtp line with them added, for whoever builds output from that
// source afterwards. Kept outside core.Codec so the producer's read loop never
// writes a field other goroutines read without locks.

type learnedParams struct {
	mu    sync.Mutex
	sps   []byte
	pps   []byte
	sprop string // "base64(sps),base64(pps)" once both are known
}

var learned sync.Map // *core.Codec -> *learnedParams

// LearnRTP inspects one RTP payload of an H.264 source (single NAL unit or
// STAP-A) and records any SPS or PPS it carries. Cheap for every other packet.
func LearnRTP(codec *core.Codec, payload []byte) {
	if codec == nil || len(payload) == 0 {
		return
	}

	var nalus [][]byte
	switch payload[0] & 0x1F {
	case NALUTypeSPS, NALUTypePPS:
		nalus = [][]byte{payload}
	case 24: // STAP-A: 1-byte header, then (16-bit size, NAL unit) pairs
		for b := payload[1:]; len(b) >= 2; {
			n := int(b[0])<<8 | int(b[1])
			b = b[2:]
			if n == 0 || n > len(b) {
				break
			}
			nalus = append(nalus, b[:n])
			b = b[n:]
		}
	default:
		return
	}

	var p *learnedParams
	for _, nalu := range nalus {
		t := nalu[0] & 0x1F
		if t != NALUTypeSPS && t != NALUTypePPS {
			continue
		}
		if p == nil {
			v, _ := learned.LoadOrStore(codec, &learnedParams{})
			p = v.(*learnedParams)
			p.mu.Lock()
			defer p.mu.Unlock()
		}
		if t == NALUTypeSPS && len(nalu) >= 4 && !bytes.Equal(p.sps, nalu) {
			p.sps = append([]byte(nil), nalu...)
		} else if t == NALUTypePPS && !bytes.Equal(p.pps, nalu) {
			p.pps = append([]byte(nil), nalu...)
		}
	}
	if p != nil && len(p.sps) > 0 && len(p.pps) > 0 {
		p.sprop = base64.StdEncoding.EncodeToString(p.sps) + "," + base64.StdEncoding.EncodeToString(p.pps)
	}
}

// LearnedFmtpLine returns codec.FmtpLine, completed with the SPS/PPS learned
// in-band when the line itself has none. profile-level-id is set from the SPS
// when missing too.
func LearnedFmtpLine(codec *core.Codec) string {
	if codec == nil {
		return ""
	}
	fmtp := codec.FmtpLine
	if codec.Name != core.CodecH264 || strings.Contains(fmtp, "sprop-parameter-sets=") {
		return fmtp
	}
	v, ok := learned.Load(codec)
	if !ok {
		return fmtp
	}
	p := v.(*learnedParams)
	p.mu.Lock()
	sprop, sps := p.sprop, p.sps
	p.mu.Unlock()
	if sprop == "" {
		return fmtp
	}

	parts := []string{}
	if fmtp != "" {
		parts = append(parts, fmtp)
	}
	if !strings.Contains(fmtp, "profile-level-id=") {
		parts = append(parts, "profile-level-id="+hex.EncodeToString(sps[1:4]))
	}
	parts = append(parts, "sprop-parameter-sets="+sprop)
	return strings.Join(parts, ";")
}

// ForgetParams drops what was learned for a source codec (its connection ended).
func ForgetParams(codec *core.Codec) {
	if codec != nil {
		learned.Delete(codec)
	}
}
