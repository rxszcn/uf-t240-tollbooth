// Package tollbooth provides rate-limiting logic to HTTP request handler.
package tollbooth

import (
	"fmt"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/didip/tollbooth/v8/errors"
	"github.com/didip/tollbooth/v8/libstring"
	"github.com/didip/tollbooth/v8/limiter"
)

// ceilSeconds converts a duration into the integer number of seconds required
// to cover it. A positive sub-second duration is rounded up to a full second so
// that clients never retry before the bucket can actually accept a request.
func ceilSeconds(d time.Duration) int64 {
	if d <= 0 {
		return 0
	}
	return int64((d + time.Second - 1) / time.Second)
}

// formatLimit renders the configured request-per-second quota without ever
// collapsing a positive fractional quota into a semantic zero. Positive values
// are rounded up (ceil); an explicitly disabled quota (max <= 0) reports zero.
func formatLimit(max float64) string {
	if max <= 0 {
		return "0"
	}
	return fmt.Sprintf("%d", int64(math.Ceil(max)))
}

// setResponseHeaders configures X-Rate-Limit-Limit and X-Rate-Limit-Duration
func setResponseHeaders(lmt *limiter.Limiter, w http.ResponseWriter, r *http.Request) {
	w.Header().Add("X-Rate-Limit-Limit", fmt.Sprintf("%.2f", lmt.GetMax()))
	w.Header().Add("X-Rate-Limit-Duration", "1")

	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if strings.TrimSpace(xForwardedFor) != "" {
		w.Header().Add("X-Rate-Limit-Request-Forwarded-For", xForwardedFor)
	}

	w.Header().Add("X-Rate-Limit-Request-Remote-Addr", r.RemoteAddr)
}

// setRateLimitResponseHeaders configures RateLimit-Limit, RateLimit-Remaining and
// RateLimit-Reset as seen at
// https://datatracker.ietf.org/doc/html/draft-ietf-httpapi-ratelimit-headers.
//
// Every value is derived from the same atomic token-bucket decision (result), so
// the advertised quota, remaining tokens and reset time always match the real
// limiter state. RateLimit-Reset is expressed as delta-seconds until the bucket
// is fully refilled. On a rejected request the matching Retry-After header (also
// delta-seconds, rounded up) is emitted so clients know exactly how long to back
// off before retrying.
func setRateLimitResponseHeaders(lmt *limiter.Limiter, w http.ResponseWriter, result limiter.LimitResult) {
	h := w.Header()
	h.Set("RateLimit-Limit", formatLimit(lmt.GetMax()))
	h.Set("RateLimit-Reset", fmt.Sprintf("%d", ceilSeconds(result.ResetDuration)))
	h.Set("RateLimit-Remaining", fmt.Sprintf("%d", result.Remaining))

	if result.Reached {
		// Retry-After is only meaningful on rejection. It mirrors the real wait
		// until the next token becomes available and is rounded up to a whole
		// second to prevent premature retries.
		h.Set("Retry-After", fmt.Sprintf("%d", ceilSeconds(result.RetryDelay)))
	}
}

// NewLimiter is a convenience function to limiter.New.
func NewLimiter(max float64, tbOptions *limiter.ExpirableOptions) *limiter.Limiter {
	return limiter.New(tbOptions).
		SetMax(max).
		SetBurst(int(math.Max(1, max)))
}

// LimitByKeys keeps track number of request made by keys separated by pipe.
// It returns HTTPError when limit is exceeded.
func LimitByKeys(lmt *limiter.Limiter, keys []string) *errors.HTTPError {
	err, _ := LimitByKeysAndReturn(lmt, keys)
	return err
}

// LimitByKeysAndReturn keeps track number of request made by keys separated by pipe.
// It returns HTTPError when limit is exceeded, and also returns the full
// token-bucket decision (remaining tokens, retry delay and reset duration).
func LimitByKeysAndReturn(lmt *limiter.Limiter, keys []string) (*errors.HTTPError, limiter.LimitResult) {
	result := lmt.LimitReachedReport(strings.Join(keys, "|"))
	if result.Reached {
		return &errors.HTTPError{Message: lmt.GetMessage(), StatusCode: lmt.GetStatusCode()}, result
	}

	return nil, result
}

// ShouldSkipLimiter is a series of filter that decides if request should be limited or not.
func ShouldSkipLimiter(lmt *limiter.Limiter, r *http.Request) bool {
	// ---------------------------------
	// Filter by remote ip
	// If we are unable to find remoteIP, skip limiter
	remoteIP := libstring.RemoteIPFromIPLookup(lmt.GetIPLookup(), r)
	remoteIP = libstring.CanonicalizeIP(remoteIP)
	if remoteIP == "" {
		return true
	}

	// ---------------------------------
	// Filter by request method
	lmtMethods := lmt.GetMethods()
	lmtMethodsIsSet := len(lmtMethods) > 0

	if lmtMethodsIsSet {
		// If request does not contain all of the methods in limiter,
		// skip limiter
		requestMethodDefinedInLimiter := libstring.StringInSlice(lmtMethods, r.Method)

		if !requestMethodDefinedInLimiter {
			return true
		}
	}

	// ---------------------------------
	// Filter by request headers
	lmtHeaders := lmt.GetHeaders()
	lmtHeadersIsSet := len(lmtHeaders) > 0

	if lmtHeadersIsSet {
		// If request does not contain all of the headers in limiter,
		// skip limiter
		requestHeadersDefinedInLimiter := false

		for headerKey := range lmtHeaders {
			reqHeaderValue := r.Header.Get(headerKey)
			if reqHeaderValue != "" {
				requestHeadersDefinedInLimiter = true
				break
			}
		}

		if !requestHeadersDefinedInLimiter {
			return true
		}

		// ------------------------------
		// If request contains the header key but not the values,
		// skip limiter
		requestHeadersDefinedInLimiter = false

		for headerKey, headerValues := range lmtHeaders {
			if len(headerValues) == 0 {
				requestHeadersDefinedInLimiter = true
				continue
			}
			for _, headerValue := range headerValues {
				if r.Header.Get(headerKey) == headerValue {
					requestHeadersDefinedInLimiter = true
					break
				}
			}
		}

		if !requestHeadersDefinedInLimiter {
			return true
		}
	}

	// ---------------------------------
	// Filter by context values
	lmtContextValues := lmt.GetContextValues()
	lmtContextValuesIsSet := len(lmtContextValues) > 0

	if lmtContextValuesIsSet {
		// If request does not contain all of the contexts in limiter,
		// skip limiter
		requestContextValuesDefinedInLimiter := false

		for contextKey := range lmtContextValues {
			reqContextValue := fmt.Sprintf("%v", r.Context().Value(contextKey))
			if reqContextValue != "" {
				requestContextValuesDefinedInLimiter = true
				break
			}
		}

		if !requestContextValuesDefinedInLimiter {
			return true
		}

		// ------------------------------
		// If request contains the context key but not the values,
		// skip limiter
		requestContextValuesDefinedInLimiter = false

		for contextKey, contextValues := range lmtContextValues {
			for _, contextValue := range contextValues {
				if r.Header.Get(contextKey) == contextValue {
					requestContextValuesDefinedInLimiter = true
					break
				}
			}
		}

		if !requestContextValuesDefinedInLimiter {
			return true
		}
	}

	// ---------------------------------
	// Filter by basic auth usernames
	lmtBasicAuthUsers := lmt.GetBasicAuthUsers()
	lmtBasicAuthUsersIsSet := len(lmtBasicAuthUsers) > 0

	if lmtBasicAuthUsersIsSet {
		// If request does not contain all of the basic auth users in limiter,
		// skip limiter
		requestAuthUsernameDefinedInLimiter := false

		username, _, ok := r.BasicAuth()
		if ok && libstring.StringInSlice(lmtBasicAuthUsers, username) {
			requestAuthUsernameDefinedInLimiter = true
		}

		if !requestAuthUsernameDefinedInLimiter {
			return true
		}
	}

	return false
}

// BuildKeys generates a slice of keys to rate-limit by given limiter and request structs.
func BuildKeys(lmt *limiter.Limiter, r *http.Request) [][]string {
	remoteIP := libstring.RemoteIPFromIPLookup(lmt.GetIPLookup(), r)
	remoteIP = libstring.CanonicalizeIP(remoteIP)
	path := r.URL.Path
	sliceKeys := make([][]string, 0)

	lmtMethods := lmt.GetMethods()
	lmtHeaders := lmt.GetHeaders()
	lmtContextValues := lmt.GetContextValues()
	lmtBasicAuthUsers := lmt.GetBasicAuthUsers()
	lmtIgnoreURL := lmt.GetIgnoreURL()

	lmtHeadersIsSet := len(lmtHeaders) > 0
	lmtContextValuesIsSet := len(lmtContextValues) > 0
	lmtBasicAuthUsersIsSet := len(lmtBasicAuthUsers) > 0

	usernameToLimit := ""
	if lmtBasicAuthUsersIsSet {
		username, _, ok := r.BasicAuth()
		if ok && libstring.StringInSlice(lmtBasicAuthUsers, username) {
			usernameToLimit = username
		}
	}

	headerValuesToLimit := [][]string{}
	if lmtHeadersIsSet {
		for headerKey, headerValues := range lmtHeaders {
			reqHeaderValue := r.Header.Get(headerKey)
			if reqHeaderValue == "" {
				continue
			}

			if len(headerValues) == 0 {
				// If header values are empty, rate-limit all request containing headerKey.
				headerValuesToLimit = append(headerValuesToLimit, []string{headerKey, reqHeaderValue})

			} else {
				// If header values are not empty, rate-limit all request with headerKey and headerValues.
				for _, headerValue := range headerValues {
					if r.Header.Get(headerKey) == headerValue {
						headerValuesToLimit = append(headerValuesToLimit, []string{headerKey, headerValue})
						break
					}
				}
			}
		}
	}

	contextValuesToLimit := [][]string{}
	if lmtContextValuesIsSet {
		for contextKey, contextValues := range lmtContextValues {
			reqContextValue := fmt.Sprintf("%v", r.Context().Value(contextKey))
			if reqContextValue == "" {
				continue
			}

			if len(contextValues) == 0 {
				// If context values are empty, rate-limit all request containing contextKey.
				contextValuesToLimit = append(contextValuesToLimit, []string{contextKey, reqContextValue})

			} else {
				// If context values are not empty, rate-limit all request with contextKey and contextValues.
				for _, contextValue := range contextValues {
					if reqContextValue == contextValue {
						contextValuesToLimit = append(contextValuesToLimit, []string{contextKey, contextValue})
						break
					}
				}
			}
		}
	}

	sliceKey := []string{remoteIP}
	if !lmtIgnoreURL {
		sliceKey = append(sliceKey, path)
	}

	sliceKey = append(sliceKey, lmtMethods...)

	for _, header := range headerValuesToLimit {
		sliceKey = append(sliceKey, header[0], header[1])
	}

	for _, contextValue := range contextValuesToLimit {
		sliceKey = append(sliceKey, contextValue[0], contextValue[1])
	}

	sliceKey = append(sliceKey, usernameToLimit)

	sliceKeys = append(sliceKeys, sliceKey)

	return sliceKeys
}

// LimitByRequest builds keys based on http.Request struct,
// loops through all the keys, and check if any one of them returns HTTPError.
func LimitByRequest(lmt *limiter.Limiter, w http.ResponseWriter, r *http.Request) *errors.HTTPError {
	setResponseHeaders(lmt, w, r)

	shouldSkip := ShouldSkipLimiter(lmt, r)
	if shouldSkip {
		return nil
	}

	sliceKeys := BuildKeys(lmt, r)

	// Aggregate the decisions over every key. On success the headers describe
	// the most constrained bucket evaluated (lowest remaining token count); on
	// rejection they describe the first bucket that actually denied the request,
	// so Retry-After/Reset reflect the real reason for the rejection.
	var worst limiter.LimitResult
	haveDecision := false

	// Loop sliceKeys and check if one of them has error.
	for _, keys := range sliceKeys {
		httpError, result := LimitByKeysAndReturn(lmt, keys)
		if !haveDecision || result.Remaining < worst.Remaining {
			worst = result
			haveDecision = true
		}
		if httpError != nil {
			setRateLimitResponseHeaders(lmt, w, result)
			return httpError
		}
	}

	setRateLimitResponseHeaders(lmt, w, worst)
	return nil
}

// LimitHandler is a middleware that performs rate-limiting given http.Handler struct.
func LimitHandler(lmt *limiter.Limiter, next http.Handler) http.Handler {
	middle := func(w http.ResponseWriter, r *http.Request) {
		httpError := LimitByRequest(lmt, w, r)
		if httpError != nil {
			lmt.ExecOnLimitReached(w, r)
			if lmt.GetOverrideDefaultResponseWriter() {
				return
			}
			w.Header().Add("Content-Type", lmt.GetMessageContentType())
			w.WriteHeader(httpError.StatusCode)
			w.Write([]byte(httpError.Message))
			return
		}

		// There's no rate-limit error, serve the next handler.
		next.ServeHTTP(w, r)
	}

	return http.HandlerFunc(middle)
}

// LimitFuncHandler is a middleware that performs rate-limiting given request handler function.
func LimitFuncHandler(lmt *limiter.Limiter, nextFunc func(http.ResponseWriter, *http.Request)) http.Handler {
	return LimitHandler(lmt, http.HandlerFunc(nextFunc))
}

// HTTPMiddleware wraps http.Handler with tollbooth limiter
func HTTPMiddleware(lmt *limiter.Limiter) func(http.Handler) http.Handler {
	// set IP lookup only if not set
	if lmt.GetIPLookup().Name == "" {
		lmt.SetIPLookup(limiter.IPLookup{Name: "RemoteAddr"})
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
				http.Error(w, "Context was canceled", http.StatusServiceUnavailable)
				return
			default:
				if httpError := LimitByRequest(lmt, w, r); httpError != nil {
					lmt.ExecOnLimitReached(w, r)
					w.Header().Add("Content-Type", lmt.GetMessageContentType())
					w.WriteHeader(httpError.StatusCode)
					w.Write([]byte(httpError.Message)) //nolint:gosec // not much we can do here with failed write
					return
				}
				next.ServeHTTP(w, r)
			}
		})
	}
}
