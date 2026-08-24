package h264

import (
	"encoding/binary"

	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/h264/annexb"
	"github.com/pion/rtp"
	"github.com/pion/rtp/codecs"
)

const RTPPacketVersionAVC = 0

const PSMaxSize = 128 // the biggest SPS I've seen is 48 (EZVIZ CS-CV210)

func RTPDepay(codec *core.Codec, handler core.HandlerFunc) core.HandlerFunc {
	depack := &codecs.H264Packet{IsAVC: true}

	sps, pps := GetParameterSet(codec.FmtpLine)
	var ps []byte
	if len(sps) > 0 && len(pps) > 0 {
		ps = JoinNALU(sps, pps)
	}

	// The source can start sending a fresh SPS/PPS in-band (e.g. after a
	// remote renegotiation changes resolution/profile, or when the original
	// offer/answer never carried sprop-parameter-sets at all) without go2rtc
	// ever seeing a new SDP. learn keeps sps/pps/ps in sync with whatever was
	// last actually seen in the stream, so a keyframe that omits its own
	// parameter set is repaired with the current set, not a stale or empty
	// one from the original negotiation. ps is only ever replaced once both
	// halves are known - an update from just one of SPS/PPS (some sources
	// send SPS once and only repeat PPS per keyframe) must not leave ps as
	// one without the other, which is worse than not repairing at all.
	learn := func(nalus [][]byte) {
		for _, nalu := range nalus {
			switch NALUType(nalu) {
			case NALUTypeSPS:
				sps = append([]byte(nil), nalu[4:]...)
			case NALUTypePPS:
				pps = append([]byte(nil), nalu[4:]...)
			}
		}
		if len(sps) > 0 && len(pps) > 0 {
			ps = JoinNALU(sps, pps)
		}
	}

	buf := make([]byte, 0, 512*1024) // 512K
	// bufDirty is true once buf holds actual NAL/Access-Unit fragment bytes
	// (a multi-packet NAL still being reassembled), as opposed to only
	// holding small buffered SPS/PPS chunks from the branch below. Only in
	// the latter case is it safe to drop buf's content instead of merging it
	// - dropping real fragment bytes would corrupt the frame.
	bufDirty := false

	return func(packet *rtp.Packet) {
		//log.Printf("[RTP] codec: %s, nalu: %2d, size: %6d, ts: %10d, pt: %2d, ssrc: %d, seq: %d, %v", codec.Name, packet.Payload[0]&0x1F, len(packet.Payload), packet.Timestamp, packet.PayloadType, packet.SSRC, packet.SequenceNumber, packet.Marker)

		payload, err := depack.Unmarshal(packet.Payload)
		if len(payload) == 0 || err != nil {
			return
		}

		// Memory overflow protection. Can happen if we miss a lot of packets with the marker.
		// https://github.com/AlexxIT/go2rtc/issues/675
		if len(buf) > 5*1024*1024 {
			buf = buf[: 0 : 512*1024]
			bufDirty = false
		}

		// Fix TP-Link Tapo TC70: sends SPS and PPS with packet.Marker = true
		// Reolink Duo 2: sends SPS with Marker and PPS without
		if packet.Marker && len(payload) < PSMaxSize {
			switch NALUType(payload) {
			case NALUTypeSPS, NALUTypePPS:
				learn(SplitNALU(payload))
				buf = append(buf, payload...)
				return
			case NALUTypeSEI:
				// RtspServer https://github.com/AlexxIT/go2rtc/issues/244
				// sends, marked SPS, marked PPS, marked SEI, marked IFrame
				return
			}
		}

		if len(buf) == 0 {
			for {
				// Amcrest IP4M-1051: 9, 7, 8, 6, 28...
				// Amcrest IP4M-1051: 9, 6, 1
				switch NALUType(payload) {
				case NALUTypeIFrame:
					// fix IFrame without SPS,PPS
					buf = append(buf, ps...)
				case NALUTypeSEI, NALUTypeAUD:
					// fix ffmpeg with transcoding first frame
					i := int(4 + binary.BigEndian.Uint32(payload))

					// check if only one NAL (fix ffmpeg transcoding for Reolink RLC-510A)
					if i == len(payload) {
						return
					}

					payload = payload[i:]
					continue
				case NALUTypePFrame, NALUTypeSPS, NALUTypePPS: // pass
				default:
					return // skip any unknown NAL unit type
				}
				break
			}
		}

		// collect all NALs for Access Unit
		if !packet.Marker {
			switch NALUType(payload) {
			case NALUTypeSPS, NALUTypePPS:
				// a standalone parameter-set NAL delivered unmarked (e.g. a
				// STAP-A aggregate that defers the marker bit to the NALU
				// that follows it) is still complete on its own, not a
				// fragment of something larger - learn from it like the
				// marked case above, and don't treat buf as holding
				// unrelated fragment data
				learn(SplitNALU(payload))
			default:
				bufDirty = true
			}
			buf = append(buf, payload...)
			return
		}

		if len(buf) > 0 {
			if !bufDirty && !(len(sps) > 0 && len(pps) > 0) {
				// buf only ever received small buffered SPS/PPS chunks above,
				// and they never formed a complete pair (e.g. a source that
				// repeats PPS but never SPS for this consumer) - injecting
				// that alone gives the decoder a parameter set reference it
				// can never resolve, worse than leaving the keyframe bare
				buf = buf[:0]
			}
			payload = append(buf, payload...)
			buf = buf[:0]
		}
		bufDirty = false

		// should not be that huge SPS
		if NALUType(payload) == NALUTypeSPS && binary.BigEndian.Uint32(payload) >= PSMaxSize {
			// some Chinese buggy cameras have a single packet with SPS+PPS+IFrame separated by 00 00 00 01
			// https://github.com/AlexxIT/WebRTC/issues/391
			// https://github.com/AlexxIT/WebRTC/issues/392
			payload = annexb.FixAnnexBInAVCC(payload)
		}

		switch NALUType(payload) {
		case NALUTypeSPS, NALUTypePPS:
			learn(SplitNALU(payload))
		}

		//log.Printf("[AVC] %v, len: %d, ts: %10d, seq: %d", NALUTypes(payload), len(payload), packet.Timestamp, packet.SequenceNumber)

		clone := *packet
		clone.Version = RTPPacketVersionAVC
		clone.Payload = payload
		handler(&clone)
	}
}

func RTPPay(mtu uint16, handler core.HandlerFunc) core.HandlerFunc {
	if mtu == 0 {
		mtu = 1472
	}

	payloader := &Payloader{IsAVC: true}
	sequencer := rtp.NewRandomSequencer()
	mtu -= 12 // rtp.Header size

	return func(packet *rtp.Packet) {
		if packet.Version != RTPPacketVersionAVC {
			handler(packet)
			return
		}

		payloads := payloader.Payload(mtu, packet.Payload)
		last := len(payloads) - 1
		for i, payload := range payloads {
			clone := rtp.Packet{
				Header: rtp.Header{
					Version:        2,
					Marker:         i == last,
					SequenceNumber: sequencer.NextSequenceNumber(),
					Timestamp:      packet.Timestamp,
				},
				Payload: payload,
			}
			handler(&clone)
		}
	}
}
