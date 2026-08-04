package streams

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

type state byte

const (
	stateNone state = iota
	stateMedias
	stateTracks
	stateStart
	stateExternal
	stateInternal
)

type Producer struct {
	core.Listener

	url      string
	template string

	conn      core.Producer
	receivers []*core.Receiver
	senders   []*core.Receiver

	state     state
	mu        sync.Mutex
	workerID  int
	failCount int
}

// First 4 consecutive failures stay at a flat 8s for transient blips. From
// the 5th on, back off exponentially (8s, 16s, 32s, ...) capped at 15
// minutes - a source failing this long is likely hitting a sustained
// external condition (e.g. an API rate limit) that retrying every few
// seconds forever won't clear any faster, and for rate-limited sources only
// prolongs the throttle window.
const (
	fastRetries = 4
	backoffBase = 8 * time.Second
	backoffCap  = 15 * time.Minute

	// minConnLifetime is how long a connection has to survive before it
	// counts as a real success and resets the failure streak. Some sources
	// (e.g. a nest camera colliding with a still-active session on Google's
	// side) will let GetProducer succeed and then die again within seconds -
	// without this, that counts as "connected" and the failure streak (and
	// its backoff) never engages, so the reconnect loop hammers the source
	// as fast as it can accept a new attempt.
	minConnLifetime = 10 * time.Second
)

func backoffDelay(failCount int) time.Duration {
	if failCount < fastRetries {
		return backoffBase
	}

	shift := failCount - fastRetries
	if shift > 12 { // backoffBase << 12 is already far past backoffCap
		shift = 12
	}

	timeout := backoffBase << uint(shift)
	if timeout > backoffCap {
		timeout = backoffCap
	}
	return timeout
}

const SourceTemplate = "{input}"

func NewProducer(source string) *Producer {
	if strings.Contains(source, SourceTemplate) {
		return &Producer{template: source}
	}

	return &Producer{url: source}
}

func (p *Producer) SetSource(s string) {
	if p.template == "" {
		p.url = s
	} else {
		p.url = strings.Replace(p.template, SourceTemplate, s, 1)
	}
}

func (p *Producer) Dial() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		conn, err := GetProducer(p.url)
		if err != nil {
			return err
		}

		p.conn = conn
		p.state = stateMedias
	}

	return nil
}

func (p *Producer) GetMedias() []*core.Media {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.conn == nil {
		return nil
	}

	return p.conn.GetMedias()
}

func (p *Producer) GetTrack(media *core.Media, codec *core.Codec) (*core.Receiver, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		return nil, errors.New("get track from none state")
	}

	for _, track := range p.receivers {
		if track.Codec == codec {
			return track, nil
		}
	}

	track, err := p.conn.GetTrack(media, codec)
	if err != nil {
		return nil, err
	}

	p.receivers = append(p.receivers, track)

	if p.state == stateMedias {
		p.state = stateTracks
	}

	return track, nil
}

func (p *Producer) AddTrack(media *core.Media, codec *core.Codec, track *core.Receiver) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state == stateNone {
		return errors.New("add track from none state")
	}

	if err := p.conn.(core.Consumer).AddTrack(media, codec, track); err != nil {
		return err
	}

	p.senders = append(p.senders, track)

	if p.state == stateMedias {
		p.state = stateTracks
	}

	return nil
}

func (p *Producer) MarshalJSON() ([]byte, error) {
	if conn := p.conn; conn != nil {
		return json.Marshal(conn)
	}
	info := map[string]string{"url": p.url}
	return json.Marshal(info)
}

// internals

func (p *Producer) start() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state != stateTracks {
		return
	}

	log.Debug().Msgf("[streams] start producer url=%s", p.url)

	p.state = stateStart
	p.workerID++

	go p.worker(p.conn, p.workerID)
}

func (p *Producer) worker(conn core.Producer, workerID int) {
	connStart := time.Now()

	if err := conn.Start(); err != nil {
		p.mu.Lock()
		closed := p.workerID != workerID
		p.mu.Unlock()

		if closed {
			return
		}

		log.Warn().Err(err).Str("url", p.url).Caller().Send()
	}

	p.armReconnect(workerID, time.Since(connStart))
}

// armReconnect decides the delay before the next reconnect attempt and
// schedules it. lived is how long the just-ended connection survived (zero
// if it never connected at all) - anything shorter than minConnLifetime
// counts as a failure and backs off; anything longer resets the streak.
func (p *Producer) armReconnect(workerID int, lived time.Duration) {
	p.mu.Lock()
	if lived >= minConnLifetime {
		p.failCount = 0
	}
	failCount := p.failCount
	p.failCount++
	p.mu.Unlock()

	delay := time.Duration(0)
	if lived < minConnLifetime {
		delay = backoffDelay(failCount)
	}

	time.AfterFunc(delay, func() {
		p.reconnect(workerID)
	})
}

func (p *Producer) reconnect(workerID int) {
	p.mu.Lock()

	if p.workerID != workerID {
		p.mu.Unlock()
		log.Trace().Msgf("[streams] stop reconnect url=%s", p.url)
		return
	}

	log.Debug().Msgf("[streams] retry=%d to url=%s", p.failCount, p.url)

	conn, err := GetProducer(p.url)
	if err != nil {
		p.mu.Unlock()
		log.Debug().Msgf("[streams] producer=%s", err)
		p.armReconnect(workerID, 0)
		return
	}

	for _, media := range conn.GetMedias() {
		switch media.Direction {
		case core.DirectionRecvonly:
			for i, receiver := range p.receivers {
				codec := media.MatchCodec(receiver.Codec)
				if codec == nil {
					continue
				}

				track, err := conn.GetTrack(media, codec)
				if err != nil {
					continue
				}

				receiver.Replace(track)
				p.receivers[i] = track
				break
			}

		case core.DirectionSendonly:
			for _, sender := range p.senders {
				codec := media.MatchCodec(sender.Codec)
				if codec == nil {
					continue
				}

				_ = conn.(core.Consumer).AddTrack(media, codec, sender)
			}
		}
	}

	// stop previous connection after moving tracks (fix ghost exec/ffmpeg)
	_ = p.conn.Stop()
	// swap connections
	p.conn = conn

	p.mu.Unlock()

	go p.worker(conn, workerID)
}

func (p *Producer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch p.state {
	case stateExternal:
		log.Trace().Msgf("[streams] skip stop external producer")
		return
	case stateNone:
		log.Trace().Msgf("[streams] skip stop none producer")
		return
	case stateStart:
		p.workerID++
	}

	// an explicit stop isn't a failure - don't let a stale streak carry a
	// backoff delay into the next time this producer is started
	p.failCount = 0

	log.Debug().Msgf("[streams] stop producer url=%s", p.url)

	if p.conn != nil {
		_ = p.conn.Stop()
		p.conn = nil
	}

	p.state = stateNone
	p.receivers = nil
	p.senders = nil
}
