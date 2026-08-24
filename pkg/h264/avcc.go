// Package h264 - AVCC format related functions
package h264

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/pion/rtp"
)

func RepairAVCC(codec *core.Codec, handler core.HandlerFunc) core.HandlerFunc {
	sps, pps := GetParameterSet(codec.FmtpLine)
	ps := JoinNALU(sps, pps)

	return func(packet *rtp.Packet) {
		// this can happen for FLV from FFmpeg
		if NALUType(packet.Payload) == NALUTypeSEI {
			size := int(binary.BigEndian.Uint32(packet.Payload)) + 4
			packet.Payload = packet.Payload[size:]
		}

		// The source can start sending fresh SPS/PPS in-band (e.g. after a
		// remote renegotiation changes resolution/profile, or when the
		// original offer/answer never carried sprop-parameter-sets at all)
		// without go2rtc seeing a new SDP. Keep sps/pps in sync with whatever
		// the source last actually sent, so a later keyframe that omits its
		// own parameter set is repaired with the current one, not a stale or
		// empty one from the original negotiation.
		switch NALUType(packet.Payload) {
		case NALUTypeSPS:
			sps = append([]byte(nil), packet.Payload[4:]...)
			// only replace ps once we have a complete, consistent pair - an
			// SPS update alone must not leave ps as an SPS with a stale or
			// missing PPS
			if len(pps) > 0 {
				ps = JoinNALU(sps, pps)
			}
		case NALUTypePPS:
			pps = append([]byte(nil), packet.Payload[4:]...)
			// same as above: some sources send SPS once and only repeat PPS
			// per keyframe, so a PPS-only update must not leave ps as a PPS
			// with no SPS, which is worse than not repairing at all
			if len(sps) > 0 {
				ps = JoinNALU(sps, pps)
			}
		case NALUTypeIFrame:
			packet.Payload = Join(ps, packet.Payload)
		}
		handler(packet)
	}
}

func JoinNALU(nalus ...[]byte) (avcc []byte) {
	var i, n int

	for _, nalu := range nalus {
		if i = len(nalu); i > 0 {
			n += 4 + i
		}
	}

	avcc = make([]byte, n)

	n = 0
	for _, nal := range nalus {
		if i = len(nal); i > 0 {
			binary.BigEndian.PutUint32(avcc[n:], uint32(i))
			n += 4 + copy(avcc[n+4:], nal)
		}
	}

	return
}

func SplitNALU(avcc []byte) [][]byte {
	var nals [][]byte
	for {
		// get AVC length
		size := int(binary.BigEndian.Uint32(avcc)) + 4

		// check if multiple items in one packet
		if size < len(avcc) {
			nals = append(nals, avcc[:size])
			avcc = avcc[size:]
		} else {
			nals = append(nals, avcc)
			break
		}
	}
	return nals
}

func NALUTypes(avcc []byte) []byte {
	var types []byte
	for {
		types = append(types, NALUType(avcc))

		size := 4 + int(binary.BigEndian.Uint32(avcc))
		if size < len(avcc) {
			avcc = avcc[size:]
		} else {
			break
		}
	}
	return types
}

func AVCCToCodec(avcc []byte) *core.Codec {
	buf := bytes.NewBufferString("packetization-mode=1")

	for {
		n := len(avcc)
		if n < 4 {
			break
		}

		size := 4 + int(binary.BigEndian.Uint32(avcc))
		if n < size {
			break
		}

		switch NALUType(avcc) {
		case NALUTypeSPS:
			buf.WriteString(";profile-level-id=")
			buf.WriteString(hex.EncodeToString(avcc[5:8]))
			buf.WriteString(";sprop-parameter-sets=")
			buf.WriteString(base64.StdEncoding.EncodeToString(avcc[4:size]))
		case NALUTypePPS:
			buf.WriteString(",")
			buf.WriteString(base64.StdEncoding.EncodeToString(avcc[4:size]))
		}

		avcc = avcc[size:]
	}

	return &core.Codec{
		Name:        core.CodecH264,
		ClockRate:   90000,
		FmtpLine:    buf.String(),
		PayloadType: core.PayloadTypeRAW,
	}
}
