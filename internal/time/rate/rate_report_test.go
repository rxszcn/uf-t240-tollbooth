// Copyright 2015 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package rate

import (
	"testing"
	"time"
)

// nearDuration compares durations with a small tolerance that absorbs float64
// representation noise (sub-microsecond) while still failing hard on order-of-
// magnitude contract errors such as a real 10s wait being advertised as 1s.
func nearDuration(got, want time.Duration) bool {
	diff := got - want
	if diff < 0 {
		diff = -diff
	}
	return diff <= 50*time.Millisecond
}

func TestAllowNReportRetryDelay(t *testing.T) {
	// 0.1 tokens/sec => one token every 10s, burst 1.
	start := time.Now()
	lim := NewLimiter(0.1, 1)

	first := lim.AllowNReport(start, 1)
	if !first.Allowed {
		t.Fatalf("first request must be allowed, got %+v", first)
	}
	if first.RetryDelay != 0 {
		t.Errorf("allowed request must report zero retry delay, got %v", first.RetryDelay)
	}
	if first.Remaining != 0 {
		t.Errorf("remaining after consuming the only token must be 0, got %d", first.Remaining)
	}

	// An immediate second request is rejected and must advertise the real wait:
	// a full 10s until the next token, not a hard-coded 1s window.
	second := lim.AllowNReport(start, 1)
	if second.Allowed {
		t.Fatalf("second immediate request must be rejected, got %+v", second)
	}
	if !nearDuration(second.RetryDelay, 10*time.Second) {
		t.Errorf("retry delay must match real 10s refill, got %v", second.RetryDelay)
	}
	// Rejection does not consume a token, so the bucket state is unchanged.
	if second.Remaining != 0 {
		t.Errorf("rejected request must leave remaining at 0, got %d", second.Remaining)
	}

	// After 4s, 0.4 token has refilled; the remaining wait for a whole token is 6s.
	at4s := lim.AllowNReport(start.Add(4*time.Second), 1)
	if at4s.Allowed {
		t.Fatalf("request after 4s must still be rejected, got %+v", at4s)
	}
	if !nearDuration(at4s.RetryDelay, 6*time.Second) {
		t.Errorf("retry delay after 4s must be 6s, got %v", at4s.RetryDelay)
	}

	// After 10s a full token is available again.
	at10s := lim.AllowNReport(start.Add(10*time.Second), 1)
	if !at10s.Allowed {
		t.Fatalf("request after 10s must be allowed, got %+v", at10s)
	}
}

func TestAllowNReportResetDuration(t *testing.T) {
	start := time.Now()
	// 1 token/sec, burst 3. Full refill of an empty bucket takes 3s.
	lim := NewLimiter(1, 3)

	// Drain all three tokens at once.
	d := lim.AllowNReport(start, 3)
	if !d.Allowed {
		t.Fatalf("burst of 3 must be allowed, got %+v", d)
	}
	if d.Remaining != 0 {
		t.Errorf("remaining after draining burst must be 0, got %d", d.Remaining)
	}
	if !nearDuration(d.ResetDuration, 3*time.Second) {
		t.Errorf("full reset of empty burst-3 bucket must be 3s, got %v", d.ResetDuration)
	}

	// After 1s exactly one token refilled. Consuming it empties the bucket again,
	// so remaining goes back to 0 and a full 3s refill is required.
	d = lim.AllowNReport(start.Add(1*time.Second), 1)
	if !d.Allowed {
		t.Fatalf("request after 1s must be allowed, got %+v", d)
	}
	if d.Remaining != 0 {
		t.Errorf("consuming the single refilled token must leave 0, got %d", d.Remaining)
	}
	if !nearDuration(d.ResetDuration, 3*time.Second) {
		t.Errorf("reset after consuming refilled token must be back to 3s, got %v", d.ResetDuration)
	}

	// A fresh, untouched bucket is full: reset is zero and a zero-token request
	// leaves it unchanged.
	full := NewLimiter(1, 3)
	d = full.AllowNReport(start, 0)
	if !d.Allowed || d.ResetDuration != 0 || d.Remaining != 3 {
		t.Errorf("fresh full bucket must report zero reset and 3 remaining, got %+v", d)
	}
}

func TestRetryDelayAndResetDoNotMutateLimiter(t *testing.T) {
	start := time.Now()
	lim := NewLimiter(1, 1)
	lim.AllowN(start, 1) // consume the only token

	// Reading the advisory values must not change subsequent decisions.
	if !nearDuration(lim.RetryDelay(), time.Second) {
		t.Errorf("RetryDelay = %v, want ~1s", lim.RetryDelay())
	}
	if !nearDuration(lim.ResetDuration(), time.Second) {
		t.Errorf("ResetDuration = %v, want ~1s", lim.ResetDuration())
	}
	if lim.AllowN(start, 1) {
		t.Error("limiter must still reject immediately after advisory reads")
	}
	if !lim.AllowN(start.Add(time.Second), 1) {
		t.Error("limiter must allow once the refill time has passed")
	}
}
