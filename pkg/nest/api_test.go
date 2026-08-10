package nest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

// syncBuffer lets the test poll captured log output from the main goroutine
// while the extend loop concurrently writes to it from its own goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestExtendDelayFloor(t *testing.T) {
	// A past or already-passed expiry must not produce a near-zero or
	// negative delay - that would hot-loop the extend goroutine against
	// the Google API.
	if d := extendDelay(time.Now().Add(-time.Hour)); d != extendMinDelay {
		t.Fatalf("expected floor of %s for a past expiry, got %s", extendMinDelay, d)
	}

	if d := extendDelay(time.Time{}); d != extendMinDelay {
		t.Fatalf("expected floor of %s for a zero expiry, got %s", extendMinDelay, d)
	}

	// A comfortably future expiry should schedule ~1 minute before it,
	// not be clamped to the floor.
	future := time.Now().Add(10 * time.Minute)
	if d := extendDelay(future); d <= extendMinDelay {
		t.Fatalf("expected delay above the floor for a far future expiry, got %s", d)
	}
}

// redirectTransport sends any request for host to a local test server
// instead of the real internet, so pkg/nest's hardcoded Google URLs can be
// exercised against a mock in-process.
type redirectTransport struct {
	host string
	next http.RoundTripper
}

func (t *redirectTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.URL.Scheme = "http"
	req.URL.Host = t.host
	req.Host = t.host
	return t.next.RoundTrip(req)
}

func withMockGoogle(t *testing.T, handler http.HandlerFunc) {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	origTransport := http.DefaultTransport
	http.DefaultTransport = &redirectTransport{host: srv.Listener.Addr().String(), next: origTransport}
	t.Cleanup(func() { http.DefaultTransport = origTransport })
}

// TestStartExtendStreamTimer_RetriesTransientFailure verifies the fix for
// the bug flagged in review on PR #2351: a transient extend failure (here,
// two 500s from Google) must not permanently kill the extend loop - it
// should retry and keep the session alive once the API recovers.
func TestStartExtendStreamTimer_RetriesTransientFailure(t *testing.T) {
	origMinDelay, origRetryDelay := extendMinDelay, extendRetryDelay
	extendMinDelay = 20 * time.Millisecond
	extendRetryDelay = 20 * time.Millisecond
	t.Cleanup(func() { extendMinDelay, extendRetryDelay = origMinDelay, origRetryDelay })

	var calls int32
	var buf syncBuffer
	origLogger := Logger
	Logger = zerolog.New(&buf)
	t.Cleanup(func() { Logger = origLogger })

	withMockGoogle(t, func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		var resv struct {
			Results struct {
				ExpiresAt      time.Time `json:"expiresAt"`
				MediaSessionID string    `json:"mediaSessionId"`
			} `json:"results"`
		}
		resv.Results.ExpiresAt = time.Now().Add(time.Hour)
		resv.Results.MediaSessionID = "session-2"
		_ = json.NewEncoder(w).Encode(resv)
	})

	a := &API{
		Token:           "tok",
		ExpiresAt:       time.Now().Add(time.Hour), // avoid exercising the oauth refresh path here
		StreamProjectID: "proj",
		StreamDeviceID:  "dev",
		StreamSessionID: "session-1",
		StreamExpiresAt: time.Now().Add(50 * time.Millisecond),
	}

	a.StartExtendStreamTimer()
	t.Cleanup(a.StopExtendStreamTimer)

	// Poll only the log output (already mutex-guarded) rather than the
	// API's Stream* fields, which the background goroutine also mutates.
	deadline := time.After(2 * time.Second)
	for {
		if strings.Contains(buf.String(), "extend stream ok") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("extend loop did not recover from transient failures: calls=%d log=%s", atomic.LoadInt32(&calls), buf.String())
		case <-time.After(5 * time.Millisecond):
		}
	}

	if !strings.Contains(buf.String(), "extend stream failed, retrying") {
		t.Fatalf("expected a retry warning to be logged, got: %s", buf.String())
	}
}

// TestStartExtendStreamTimer_GivesUpAndLogs verifies that permanent
// failures still stop the loop (so it doesn't retry forever against a dead
// refresh token) but now do so loudly instead of a silent return.
func TestStartExtendStreamTimer_GivesUpAndLogs(t *testing.T) {
	origMinDelay, origRetryDelay, origMaxRetries := extendMinDelay, extendRetryDelay, extendMaxRetries
	extendMinDelay = 5 * time.Millisecond
	extendRetryDelay = 5 * time.Millisecond
	extendMaxRetries = 3
	t.Cleanup(func() {
		extendMinDelay, extendRetryDelay, extendMaxRetries = origMinDelay, origRetryDelay, origMaxRetries
	})

	var buf syncBuffer
	origLogger := Logger
	Logger = zerolog.New(&buf)
	t.Cleanup(func() { Logger = origLogger })

	withMockGoogle(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})

	a := &API{
		Token:           "tok",
		ExpiresAt:       time.Now().Add(time.Hour),
		StreamProjectID: "proj",
		StreamDeviceID:  "dev",
		StreamSessionID: "session-1",
		StreamExpiresAt: time.Now().Add(5 * time.Millisecond),
	}

	a.StartExtendStreamTimer()
	t.Cleanup(a.StopExtendStreamTimer)

	deadline := time.After(2 * time.Second)
	for {
		if strings.Contains(buf.String(), "giving up") {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("expected the extend loop to give up and log it, got: %s", buf.String())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// TestExchangeSDP_RetriesOn429AndSucceeds directly answers "does the 429
// backoff retry still work": Google returns 429 twice, and the call must
// advance through attempt=1, attempt=2 and then succeed on attempt=3,
// rather than giving up (or silently returning) after a single attempt.
func TestExchangeSDP_RetriesOn429AndSucceeds(t *testing.T) {
	origMaxRetries, origRetryDelay := sdpMaxRetries, sdpRetryDelay
	sdpMaxRetries = 3
	sdpRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { sdpMaxRetries, sdpRetryDelay = origMaxRetries, origRetryDelay })

	var buf syncBuffer
	origLogger := Logger
	Logger = zerolog.New(&buf)
	t.Cleanup(func() { Logger = origLogger })

	var sdpCalls int32
	withMockGoogle(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "oauth2") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "tok", "expires_in": 3600,
			})
			return
		}

		n := atomic.AddInt32(&sdpCalls, 1)
		if n <= 2 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}

		var resv struct {
			Results struct {
				Answer         string    `json:"answerSdp"`
				ExpiresAt      time.Time `json:"expiresAt"`
				MediaSessionID string    `json:"mediaSessionId"`
			} `json:"results"`
		}
		resv.Results.Answer = "answer-sdp"
		resv.Results.ExpiresAt = time.Now().Add(5 * time.Minute)
		resv.Results.MediaSessionID = "session-1"
		_ = json.NewEncoder(w).Encode(resv)
	})

	a, err := NewAPI("cid", "csecret", "rtoken")
	if err != nil {
		t.Fatalf("NewAPI: %v", err)
	}

	answer, err := a.ExchangeSDP("proj", "dev", "offer-sdp")
	if err != nil {
		t.Fatalf("ExchangeSDP: %v", err)
	}
	if answer != "answer-sdp" {
		t.Fatalf("unexpected answer: %q", answer)
	}
	if atomic.LoadInt32(&sdpCalls) != 3 {
		t.Fatalf("expected exactly 3 sdp calls (2 rejected + 1 success), got %d", sdpCalls)
	}

	log := buf.String()
	if strings.Count(log, "exchange sdp rejected, retrying") != 2 {
		t.Fatalf("expected 2 retry warnings, got log: %s", log)
	}
	if !strings.Contains(log, `"attempt":1`) || !strings.Contains(log, `"attempt":2`) {
		t.Fatalf("expected attempt numbers to advance from 1 to 2, got log: %s", log)
	}
}

// TestExchangeSDP_GivesUpAfterMaxRetriesOn429 verifies persistent 429s
// exhaust all attempts and return a clear error instead of retrying forever
// or silently failing after just one try.
func TestExchangeSDP_GivesUpAfterMaxRetriesOn429(t *testing.T) {
	origMaxRetries, origRetryDelay := sdpMaxRetries, sdpRetryDelay
	sdpMaxRetries = 3
	sdpRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { sdpMaxRetries, sdpRetryDelay = origMaxRetries, origRetryDelay })

	var buf syncBuffer
	origLogger := Logger
	Logger = zerolog.New(&buf)
	t.Cleanup(func() { Logger = origLogger })

	var sdpCalls int32
	withMockGoogle(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "oauth2") {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": "tok", "expires_in": 3600,
			})
			return
		}
		atomic.AddInt32(&sdpCalls, 1)
		w.WriteHeader(http.StatusTooManyRequests)
	})

	a, err := NewAPI("cid2", "csecret2", "rtoken2")
	if err != nil {
		t.Fatalf("NewAPI: %v", err)
	}

	_, err = a.ExchangeSDP("proj", "dev", "offer-sdp")
	if err == nil {
		t.Fatal("expected an error after exhausting retries, got nil")
	}
	if atomic.LoadInt32(&sdpCalls) != int32(sdpMaxRetries) {
		t.Fatalf("expected exactly %d sdp calls, got %d", sdpMaxRetries, sdpCalls)
	}

	log := buf.String()
	if strings.Count(log, "exchange sdp rejected, retrying") != sdpMaxRetries {
		t.Fatalf("expected %d retry warnings, got log: %s", sdpMaxRetries, log)
	}
}
