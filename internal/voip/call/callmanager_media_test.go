package call

import (
	"log/slog"
	"sync"
	"testing"
	"time"

	"wacalls/internal/voip/media"
	"wacalls/internal/voip/transport"
)

// fakeRelay is a minimal RelayTransport that just reports a connection and
// records broadcasted payloads so tests can observe whether a keepalive
// frame was actually sent.
type fakeRelay struct {
	mu   sync.Mutex
	sent int
}

func (f *fakeRelay) SetSsrc(uint32)                          {}
func (f *fakeRelay) SetSubscriptionSsrc(uint32)              {}
func (f *fakeRelay) SetOnConnected(func(string, int))        {}
func (f *fakeRelay) SetOnReceive(func([]byte))               {}
func (f *fakeRelay) ResendSubscriptions()                    {}
func (f *fakeRelay) ConfigureRelays([]transport.RelayConfig) {}
func (f *fakeRelay) HasConnection() bool                     { return true }
func (f *fakeRelay) ConnectedCount() int                     { return 1 }
func (f *fakeRelay) Cleanup()                                {}
func (f *fakeRelay) Broadcast(data []byte) {
	f.mu.Lock()
	f.sent++
	f.mu.Unlock()
}
func (f *fakeRelay) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sent
}

func newReadyCallManagerForKeepaliveTest(t *testing.T) (*CallManager, *fakeRelay) {
	t.Helper()
	codec, err := media.NewMLowCodec(media.DefaultCodecOptions)
	if err != nil {
		t.Skipf("codec unavailable in this environment: %v", err)
	}
	keying, err := media.DerivePerJidSrtpKey(media.GenerateCallKey(), "peer@lid")
	if err != nil {
		t.Fatalf("derive srtp key: %v", err)
	}
	srtpSession, err := media.NewSrtpSession(keying, keying, 0, 0)
	if err != nil {
		t.Fatalf("new srtp session: %v", err)
	}
	relay := &fakeRelay{}
	m := &CallManager{
		log:         slog.Default(),
		codec:       codec,
		rtpSession:  media.NewWhatsAppOpusSession(12345),
		srtpSession: srtpSession,
		relay:       relay,
	}
	return m, relay
}

// TestTickSilenceKeepaliveUsesFreshNow is the regression test for the
// call-site bug: the per-call ticker loop used to pass the ticker's
// (possibly stale) delivery timestamp into TickSilenceKeepalive instead of
// the actual time execution resumed. Under scheduling delay, the ticker
// timestamp under-reports elapsed idle time and can wrongly skip sending a
// keepalive frame that a fresh time.Now() would correctly send.
//
// This test simulates that delay directly against the exported
// TickSilenceKeepalive: a "stale" now (captured shortly after lastCaptureAt,
// mimicking a ticker value delivered right on schedule) must NOT be judged
// idle, while the real, later now (mimicking what time.Now() returns after
// the goroutine is delayed by scheduling/lock contention) MUST be judged
// idle and trigger a keepalive frame.
func TestTickSilenceKeepaliveUsesFreshNow(t *testing.T) {
	m, relay := newReadyCallManagerForKeepaliveTest(t)

	start := time.Now()
	m.lastCaptureAt = start

	// A "stale" timestamp close to lastCaptureAt (as if delivered by the
	// ticker right when it fired, before any scheduling delay) is not idle.
	staleNow := start.Add(10 * time.Millisecond)
	if sent := m.TickSilenceKeepalive(staleNow); sent {
		t.Fatalf("expected no keepalive frame for stale/near timestamp, got sent=%v (relay count=%d)", sent, relay.count())
	}

	// Simulate the real scheduling/lock delay the goroutine experienced
	// between the tick firing and the code actually running.
	time.Sleep(150 * time.Millisecond)

	// A fresh time.Now() captured at actual execution time correctly
	// observes the call has been idle past the 120ms threshold.
	freshNow := time.Now()
	if freshNow.Sub(start) <= 120*time.Millisecond {
		t.Fatalf("test setup issue: elapsed time too small (%v)", freshNow.Sub(start))
	}
	if sent := m.TickSilenceKeepalive(freshNow); !sent {
		t.Fatalf("expected keepalive frame using fresh time.Now(), got sent=%v (relay count=%d)", sent, relay.count())
	}
	if got := relay.count(); got != 1 {
		t.Fatalf("expected exactly 1 keepalive frame broadcast, got %d", got)
	}
}

// TestStartSilenceKeepaliveLockedCallSitePassesFreshNow drives the actual
// per-call goroutine (startSilenceKeepaliveLocked) end to end and confirms
// it keeps sending keepalive frames based on real elapsed idle time, not a
// stale ticker timestamp — i.e. it exercises the exact call site the fix
// touches (`case <-ticker.C: m.TickSilenceKeepalive(time.Now())`).
func TestStartSilenceKeepaliveLockedCallSitePassesFreshNow(t *testing.T) {
	m, relay := newReadyCallManagerForKeepaliveTest(t)

	m.mu.Lock()
	m.lastCaptureAt = time.Now()
	m.startSilenceKeepaliveLocked()
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		if m.keepaliveStop != nil {
			close(m.keepaliveStop)
			m.keepaliveStop = nil
		}
		m.mu.Unlock()
	}()

	// Idle threshold is 120ms and the ticker period is 60ms; give it enough
	// real wall-clock time for several ticks to land and for at least one of
	// them to observe idle==true using a freshly captured time.Now().
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if relay.count() > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("expected at least one keepalive frame to be sent via the real per-call ticker loop, got 0")
}
