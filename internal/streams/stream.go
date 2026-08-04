package streams

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexxIT/go2rtc/pkg/core"
)

// stopGracePeriod is how long a producer is kept running with no consumers
// before it's actually stopped. Some consumers (e.g. an NVR's RTSP client)
// reconnect every few seconds; without a grace period, every reconnect tears
// down and rebuilds the whole producer, which for API-backed sources (nest)
// means a full remote session teardown+recreate on every reconnect - wasted
// work, and for rate-limited APIs, actively harmful.
const stopGracePeriod = 3 * time.Minute

type Stream struct {
	producers []*Producer
	consumers []core.Consumer
	mu        sync.Mutex
	pending   atomic.Int32
	stopTimer *time.Timer
}

func NewStream(source any) *Stream {
	switch source := source.(type) {
	case string:
		return &Stream{
			producers: []*Producer{NewProducer(source)},
		}
	case []string:
		s := new(Stream)
		for _, str := range source {
			s.producers = append(s.producers, NewProducer(str))
		}
		return s
	case []any:
		s := new(Stream)
		for _, src := range source {
			str, ok := src.(string)
			if !ok {
				log.Error().Msgf("[stream] NewStream: Expected string, got %v", src)
				continue
			}
			s.producers = append(s.producers, NewProducer(str))
		}
		return s
	case map[string]any:
		return NewStream(source["url"])
	case nil:
		return new(Stream)
	default:
		panic(core.Caller())
	}
}

func (s *Stream) Sources() []string {
	sources := make([]string, 0, len(s.producers))
	for _, prod := range s.producers {
		sources = append(sources, prod.url)
	}
	return sources
}

func (s *Stream) SetSource(source string) {
	for _, prod := range s.producers {
		prod.SetSource(source)
	}
}

func (s *Stream) RemoveConsumer(cons core.Consumer) {
	_ = cons.Stop()

	s.mu.Lock()
	for i, consumer := range s.consumers {
		if consumer == cons {
			s.consumers = append(s.consumers[:i], s.consumers[i+1:]...)
			break
		}
	}
	s.mu.Unlock()

	s.scheduleStopProducers()
}

// scheduleStopProducers arms stopProducers to run after stopGracePeriod,
// replacing any timer already pending. AddConsumer cancels this if a new
// consumer shows up before it fires.
func (s *Stream) scheduleStopProducers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopTimer != nil {
		s.stopTimer.Stop()
	}

	s.stopTimer = time.AfterFunc(stopGracePeriod, func() {
		s.mu.Lock()
		s.stopTimer = nil
		s.mu.Unlock()

		s.stopProducers()
	})
}

func (s *Stream) AddProducer(prod core.Producer) {
	producer := &Producer{conn: prod, state: stateExternal, url: "external"}
	s.mu.Lock()
	s.producers = append(s.producers, producer)
	s.mu.Unlock()
}

func (s *Stream) RemoveProducer(prod core.Producer) {
	s.mu.Lock()
	for i, producer := range s.producers {
		if producer.conn == prod {
			s.producers = append(s.producers[:i], s.producers[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
}

func (s *Stream) stopProducers() {
	if s.pending.Load() > 0 {
		log.Trace().Msg("[streams] skip stop pending producer")
		return
	}

	s.mu.Lock()
producers:
	for _, producer := range s.producers {
		for _, track := range producer.receivers {
			if len(track.Senders()) > 0 {
				continue producers
			}
		}
		for _, track := range producer.senders {
			if len(track.Senders()) > 0 {
				continue producers
			}
		}
		producer.stop()
	}
	s.mu.Unlock()
}

func (s *Stream) MarshalJSON() ([]byte, error) {
	var info = struct {
		Producers []*Producer     `json:"producers"`
		Consumers []core.Consumer `json:"consumers"`
	}{
		Producers: s.producers,
		Consumers: s.consumers,
	}
	return json.Marshal(info)
}
