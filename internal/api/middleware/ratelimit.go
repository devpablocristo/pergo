package middleware

import (
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"golang.org/x/time/rate"

	"github.com/pablojhp.pergo/internal/domain"
	"github.com/pablojhp.pergo/internal/platform/postgres/tenant"
)

// RateLimiter provides per-workspace token-bucket rate limiting.
// Each workspace gets an independent rate.Limiter stored in a sync.Map.
type RateLimiter struct {
	limiters sync.Map // workspace_id uuid.UUID → *rate.Limiter
	rate     rate.Limit
	burst    int
}

type ipLimiterEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// IPRateLimiter protects unauthenticated endpoints such as the admin login.
// Its key map is bounded so spoofed source addresses cannot grow memory
// indefinitely. The deployment edge should enforce an additional shared limit.
type IPRateLimiter struct {
	mu         sync.Mutex
	entries    map[string]*ipLimiterEntry
	rate       rate.Limit
	burst      int
	maxEntries int
}

func NewIPRateLimiter(rps float64, burst int) *IPRateLimiter {
	return &IPRateLimiter{
		entries:    make(map[string]*ipLimiterEntry),
		rate:       rate.Limit(rps),
		burst:      burst,
		maxEntries: 4096,
	}
}

func (rl *IPRateLimiter) Allow(ip string) bool {
	now := time.Now()
	rl.mu.Lock()
	defer rl.mu.Unlock()

	entry, ok := rl.entries[ip]
	if !ok {
		if len(rl.entries) >= rl.maxEntries {
			rl.evictOldest()
		}
		entry = &ipLimiterEntry{limiter: rate.NewLimiter(rl.rate, rl.burst)}
		rl.entries[ip] = entry
	}
	entry.lastSeen = now
	return entry.limiter.Allow()
}

func (rl *IPRateLimiter) evictOldest() {
	var oldestKey string
	var oldestTime time.Time
	for key, entry := range rl.entries {
		if oldestKey == "" || entry.lastSeen.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.lastSeen
		}
	}
	if oldestKey != "" {
		delete(rl.entries, oldestKey)
	}
}

func IPRateLimiterMiddleware(rl *IPRateLimiter) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if !rl.Allow(c.RealIP()) {
				c.Response().Header().Set("Retry-After", "5")
				return c.String(http.StatusTooManyRequests, "too many login attempts")
			}
			return next(c)
		}
	}
}

// NewRateLimiter creates a rate limiter with the given requests-per-second
// rate and burst size. Each workspace gets an independent token bucket.
func NewRateLimiter(rps float64, burst int) *RateLimiter {
	return &RateLimiter{
		rate:  rate.Limit(rps),
		burst: burst,
	}
}

// Allow returns true if the workspace has a token available. On false, the
// caller should return HTTP 429 with a Retry-After header.
func (rl *RateLimiter) Allow(workspaceID uuid.UUID) bool {
	limiter := rl.getLimiter(workspaceID)
	return limiter.Allow()
}

// getLimiter loads or creates a rate.Limiter for the given workspace.
func (rl *RateLimiter) getLimiter(workspaceID uuid.UUID) *rate.Limiter {
	if limiter, ok := rl.limiters.Load(workspaceID); ok {
		return limiter.(*rate.Limiter)
	}
	limiter := rate.NewLimiter(rl.rate, rl.burst)
	actual, _ := rl.limiters.LoadOrStore(workspaceID, limiter)
	return actual.(*rate.Limiter)
}

// RateLimiterMiddleware returns an Echo middleware that enforces per-workspace
// rate limiting. If the workspace_id is not in the context (auth middleware
// hasn't run), the request passes through.
func RateLimiterMiddleware(rl *RateLimiter) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			workspaceID, ok := tenant.WorkspaceIDFrom(c.Request().Context())
			if !ok {
				return next(c)
			}

			if !rl.Allow(workspaceID) {
				c.Response().Header().Set("Retry-After", "1")
				return c.JSON(http.StatusTooManyRequests, domain.ErrorResponse{
					Code:     "rate_limited",
					Message:  "too many requests, slow down",
					MoreInfo: "https://docs.pergo.dev/errors/rate_limited",
				})
			}

			return next(c)
		}
	}
}

// QueueDepthTracker provides per-workspace message queue depth tracking
// using atomic counters in a sync.Map.
type QueueDepthTracker struct {
	depths sync.Map // workspace_id uuid.UUID → *int64
}

// NewQueueDepthTracker creates a new queue depth tracker.
func NewQueueDepthTracker() *QueueDepthTracker {
	return &QueueDepthTracker{}
}

// Increment atomically increases the queue depth counter for a workspace.
func (qdt *QueueDepthTracker) Increment(workspaceID uuid.UUID) {
	counter := qdt.getOrCreate(workspaceID)
	atomic.AddInt64(counter, 1)
}

// Decrement atomically decreases the queue depth counter for a workspace.
// The counter floors at 0 (never goes negative).
func (qdt *QueueDepthTracker) Decrement(workspaceID uuid.UUID) {
	counter := qdt.getOrCreate(workspaceID)
	for {
		current := atomic.LoadInt64(counter)
		if current <= 0 {
			return
		}
		if atomic.CompareAndSwapInt64(counter, current, current-1) {
			return
		}
	}
}

// Depth returns the current queue depth for a workspace.
func (qdt *QueueDepthTracker) Depth(workspaceID uuid.UUID) int64 {
	counter := qdt.getOrCreate(workspaceID)
	return atomic.LoadInt64(counter)
}

// Exceeds returns true if the workspace's queue depth is >= the given limit.
func (qdt *QueueDepthTracker) Exceeds(workspaceID uuid.UUID, limit int64) bool {
	return qdt.Depth(workspaceID) >= limit
}

// getOrCreate loads or creates an atomic counter for the given workspace.
func (qdt *QueueDepthTracker) getOrCreate(workspaceID uuid.UUID) *int64 {
	if counter, ok := qdt.depths.Load(workspaceID); ok {
		return counter.(*int64)
	}
	var zero int64
	counter := &zero
	actual, _ := qdt.depths.LoadOrStore(workspaceID, counter)
	return actual.(*int64)
}

// Ensure rate.Limiter is used (compile-time check).
var _ = rate.Every(time.Second)
