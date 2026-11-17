package limiter

import (
	"testing"
	"time"
)

// TestLimitReachedReportExposesRealWait covers the fractional-rate contract:
// below 0.5 req/s the quota must never be reported as a semantic zero, and the
// retry/reset durations advertised to clients must match the real token-bucket
// refill time (10s for a 0.1 req/s bucket), not a hard-coded 1s window.
func TestLimitReachedReportExposesRealWait(t *testing.T) {
	// 0.1 request/second => one token every 10 seconds, burst 1.
	lmt := New(nil).SetMax(0.1).SetBurst(1)
	key := "127.0.0.1|/"

	first := lmt.LimitReachedReport(key)
	if first.Reached {
		t.Fatalf("first request must be allowed, got %+v", first)
	}
	if first.RetryDelay != 0 {
		t.Errorf("allowed request must report zero retry delay, got %v", first.RetryDelay)
	}
	if first.Remaining != 0 {
		t.Errorf("remaining after consuming the only token must be 0, got %d", first.Remaining)
	}
	// An empty burst-1 bucket refills one token in 10s at 0.1 tok/s.
	if !around(first.ResetDuration, 10*time.Second) {
		t.Errorf("reset duration must be ~10s, got %v", first.ResetDuration)
	}

	second := lmt.LimitReachedReport(key)
	if !second.Reached {
		t.Fatalf("immediate second request must be rejected, got %+v", second)
	}
	if second.Remaining != 0 {
		t.Errorf("rejected request must leave remaining at 0, got %d", second.Remaining)
	}
	// The advertised backoff has to match the real wait (within scheduling slack),
	// i.e. ~10s rather than the old hard-coded 1s.
	if !around(second.RetryDelay, 10*time.Second) {
		t.Errorf("retry delay must be ~10s to match real wait, got %v", second.RetryDelay)
	}
	if !around(second.ResetDuration, 10*time.Second) {
		t.Errorf("reset duration on rejection must be ~10s, got %v", second.ResetDuration)
	}
	if second.RetryDelay < 9*time.Second {
		t.Errorf("retry delay %v is far below the real 10s refill; clients would retry too early", second.RetryDelay)
	}
}

// TestLimitReachedReportIntegerRate keeps the classic 1 req/s behavior honest.
func TestLimitReachedReportIntegerRate(t *testing.T) {
	lmt := New(nil).SetMax(1).SetBurst(1)
	key := "127.0.0.1|/"

	if r := lmt.LimitReachedReport(key); r.Reached || r.RetryDelay != 0 || r.Remaining != 0 {
		t.Fatalf("first request should be allowed with no backoff, got %+v", r)
	}
	r := lmt.LimitReachedReport(key)
	if !r.Reached {
		t.Fatalf("second immediate request must be rejected, got %+v", r)
	}
	if !around(r.RetryDelay, time.Second) {
		t.Errorf("retry delay for a 1 req/s bucket must be ~1s, got %v", r.RetryDelay)
	}
}

// around reports whether d is within 0.5s of want, absorbing the small wall
// clock gap between the limiter's internal time.Now() and the assertion.
func around(d, want time.Duration) bool {
	diff := d - want
	if diff < 0 {
		diff = -diff
	}
	return diff <= 500*time.Millisecond
}
