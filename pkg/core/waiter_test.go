package core

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// Done used to release the waiters (WaitGroup.Done) before storing err, and
// Wait reads w.err without the lock, so a waiter could return nil for a
// connection that ended with an error. Run with -race to see the data race;
// the value check catches the lost error even without it.
func TestWaiter_WaitReturnsTheErrorPassedToDone(t *testing.T) {
	want := errors.New("connection failed")

	for i := 0; i < 2000; i++ {
		var w Waiter
		got := make(chan error, 1)
		started := make(chan struct{})

		go func() {
			close(started)
			got <- w.Wait()
		}()
		<-started
		// make sure Wait has registered (state 1) before Done runs
		for {
			w.mu.Lock()
			s := w.state
			w.mu.Unlock()
			if s == 1 {
				break
			}
		}

		w.Done(want)
		require.ErrorIs(t, <-got, want, "iteration %d", i)
	}
}

// A second Done must stay a harmless no-op (webrtc.Conn.Close can run twice:
// once from the read loop and once from OnConnectionStateChange).
func TestWaiter_DoubleDoneIsSafe(t *testing.T) {
	var w Waiter
	done := make(chan error, 1)
	go func() { done <- w.Wait() }()
	for {
		w.mu.Lock()
		s := w.state
		w.mu.Unlock()
		if s == 1 {
			break
		}
	}
	first := errors.New("first")
	w.Done(first)
	w.Done(errors.New("second"))
	require.ErrorIs(t, <-done, first)
	require.ErrorIs(t, w.Wait(), first) // later waiters see the same result
}
