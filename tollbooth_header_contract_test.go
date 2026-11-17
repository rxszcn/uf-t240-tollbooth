package tollbooth

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/didip/tollbooth/v8/limiter"
)

func header(v http.Header, key string) string {
	values := v[http.CanonicalHeaderKey(key)]
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// TestFractionalRateHeadersAreTruthful exercises the exact failure from the
// report: with a 0.1 req/s quota (one token per 10s) the headers used to claim a
// 1s reset and a quota rounded down to zero. They must now reflect the real
// 10-second wait and never emit a semantic zero quota.
func TestFractionalRateHeadersAreTruthful(t *testing.T) {
	lmt := NewLimiter(0.1, nil) // one request per 10 seconds
	lmt.SetIPLookup(limiter.IPLookup{Name: "RemoteAddr"})

	handler := HTTPMiddleware(lmt)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	newReq := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/test", nil)
		r.RemoteAddr = "127.0.0.1:12345"
		return r
	}

	// First request: allowed. No Retry-After on success.
	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, newReq())
	if w1.Code != http.StatusOK {
		t.Fatalf("first request: expected 200, got %d", w1.Code)
	}
	if got := header(w1.Header(), "Retry-After"); got != "" {
		t.Errorf("successful response must not carry Retry-After, got %q", got)
	}
	// A positive fractional quota must round up to 1, never collapse to 0.
	if got := header(w1.Header(), "RateLimit-Limit"); got != "1" {
		t.Errorf("RateLimit-Limit must not be a semantic zero for max=0.1, got %q", got)
	}

	// Immediate second request: rejected.
	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, newReq())
	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request: expected 429, got %d", w2.Code)
	}

	h2 := w2.Header()
	// 429 must always include a backoff hint and it has to match the real ~10s
	// wait, not the old hard-coded 1s.
	if got := header(h2, "Retry-After"); got != "10" {
		t.Errorf("Retry-After must match real 10s wait, got %q", got)
	}
	if got := header(h2, "RateLimit-Reset"); got != "10" {
		t.Errorf("RateLimit-Reset must match real 10s refill, got %q", got)
	}
	if got := header(h2, "RateLimit-Limit"); got != "1" {
		t.Errorf("RateLimit-Limit must be ceil(0.1)=1, got %q", got)
	}
	if got := header(h2, "RateLimit-Remaining"); got != "0" {
		t.Errorf("RateLimit-Remaining must be 0 while empty, got %q", got)
	}
}

// TestIntegerRateRetryAfter verifies the classic 1 req/s limiter advertises a
// one-second backoff on rejection and no Retry-After while requests are allowed.
func TestIntegerRateRetryAfter(t *testing.T) {
	lmt := NewLimiter(1, nil)
	lmt.SetIPLookup(limiter.IPLookup{Name: "RemoteAddr"})

	handler := HTTPMiddleware(lmt)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	newReq := func() *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/test", nil)
		r.RemoteAddr = "127.0.0.1:6789"
		return r
	}

	w1 := httptest.NewRecorder()
	handler.ServeHTTP(w1, newReq())
	if w1.Code != http.StatusOK || header(w1.Header(), "Retry-After") != "" {
		t.Fatalf("first request should be allowed without Retry-After, code=%d header=%q",
			w1.Code, header(w1.Header(), "Retry-After"))
	}

	w2 := httptest.NewRecorder()
	handler.ServeHTTP(w2, newReq())
	if w2.Code != http.StatusTooManyRequests {
		t.Fatalf("second request should be rejected, got %d", w2.Code)
	}
	if got := header(w2.Header(), "Retry-After"); got != "1" {
		t.Errorf("Retry-After for 1 req/s bucket must be 1, got %q", got)
	}
	if got := header(w2.Header(), "RateLimit-Reset"); got != "1" {
		t.Errorf("RateLimit-Reset for empty 1 req/s burst-1 bucket must be 1, got %q", got)
	}
}
