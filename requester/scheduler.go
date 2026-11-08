// Copyright 2024 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package requester

import (
	"errors"
	"net/http"
	"sync"
	"time"
)

// Target describes a weighted request target used by the multi-target
// fair scheduler.
type Target struct {
	// Request is the template request sent to this target. It is cloned
	// for every dispatched request.
	Request *http.Request

	// Weight determines the target's share of the traffic. It must be
	// non-negative. A target with zero weight never receives requests.
	// At least one target in a scheduler must have a positive weight.
	Weight int

	// MaxConcurrent caps the number of in-flight requests to this
	// target. Zero means no per-target cap.
	MaxConcurrent int
}

// Circuit breaker states of a single target.
const (
	breakerClosed   = iota // traffic flows according to the weight
	breakerOpen            // all traffic is rejected until the window ends
	breakerHalfOpen        // a single probe request is in flight
)

// Scheduler dispatches requests across weighted targets.
//
// The target of each request is chosen with the smooth weighted
// round-robin algorithm (the same deterministic algorithm used by
// nginx): for static weights it produces a fixed, evenly interleaved
// sequence that depends only on the weights, never on goroutine
// scheduling or map iteration.
//
// A target whose request fails has its circuit opened for a breaker
// window: its effective weight drops to zero and it receives no
// traffic. When the window ends, exactly one probe request is allowed;
// a successful probe restores the target's full weight while a failed
// probe re-opens the circuit for another window.
//
// All methods are safe for concurrent use.
type Scheduler struct {
	mu   sync.Mutex
	cond *sync.Cond

	reqs []*http.Request
	// base weights and per-target concurrency caps (0 = no cap)
	weights []int
	caps    []int

	// current holds the smooth weighted round-robin dynamic weights.
	current []int
	// inFlight is the number of requests currently dispatched per target.
	inFlight []int
	// breaker state, open deadline and probe flag per target.
	state     []int
	openUntil []time.Time
	probing   []bool

	// remaining is the number of requests still to dispatch.
	remaining int64
	stopped   bool

	// window is the circuit-breaker window. Zero disables breaking.
	window time.Duration
	// nowFunc returns the current time; it is overridable in tests.
	nowFunc func() time.Time
}

// NewScheduler builds a Scheduler that dispatches total requests across
// the given targets. window is the circuit-breaker window applied to
// failing targets; a zero window disables the circuit breaker.
func NewScheduler(targets []Target, total int64, window time.Duration) (*Scheduler, error) {
	if len(targets) == 0 {
		return nil, errors.New("scheduler: at least one target is required")
	}
	if total < 0 {
		return nil, errors.New("scheduler: total number of requests cannot be negative")
	}
	if window < 0 {
		return nil, errors.New("scheduler: breaker window cannot be negative")
	}

	n := len(targets)
	s := &Scheduler{
		reqs:      make([]*http.Request, n),
		weights:   make([]int, n),
		caps:      make([]int, n),
		current:   make([]int, n),
		inFlight:  make([]int, n),
		state:     make([]int, n),
		openUntil: make([]time.Time, n),
		probing:   make([]bool, n),
		remaining: total,
		window:    window,
		nowFunc:   time.Now,
	}
	positive := false
	for i, t := range targets {
		if t.Request == nil {
			return nil, errors.New("scheduler: target request cannot be nil")
		}
		if t.Weight < 0 {
			return nil, errors.New("scheduler: target weight cannot be negative")
		}
		if t.MaxConcurrent < 0 {
			return nil, errors.New("scheduler: target concurrency cap cannot be negative")
		}
		if t.Weight > 0 {
			positive = true
		}
		s.reqs[i] = t.Request
		s.weights[i] = t.Weight
		s.caps[i] = t.MaxConcurrent
	}
	if !positive {
		return nil, errors.New("scheduler: at least one target must have a positive weight")
	}
	s.cond = sync.NewCond(&s.mu)
	return s, nil
}

// Next blocks until a target can serve a request and returns its index.
// The second return value is false when the scheduler has been stopped
// or all requests have been dispatched, in which case the caller must
// not call Complete.
func (s *Scheduler) Next() (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for {
		if s.stopped || s.remaining <= 0 {
			return -1, false
		}
		now := s.nowFunc()

		// A target whose breaker window has ended gets a single probe
		// before any normal traffic is considered.
		if idx := s.pickProbe(now); idx >= 0 {
			s.state[idx] = breakerHalfOpen
			s.probing[idx] = true
			s.inFlight[idx]++
			s.remaining--
			return idx, true
		}

		// Otherwise dispatch with smooth weighted round-robin.
		if idx := s.pickWeighted(); idx >= 0 {
			s.inFlight[idx]++
			s.remaining--
			return idx, true
		}

		// Every eligible target is either capped or circuit-opened.
		// Wait for a completion, a stop signal, or the next window end.
		s.waitForChange()
	}
}

// pickProbe returns the index of a target whose open window has ended
// and that still has concurrency capacity, or -1. The lowest index
// wins so the choice is deterministic.
func (s *Scheduler) pickProbe(now time.Time) int {
	for i := range s.reqs {
		if s.state[i] != breakerOpen || s.probing[i] {
			continue
		}
		if now.Before(s.openUntil[i]) {
			continue
		}
		if s.caps[i] > 0 && s.inFlight[i] >= s.caps[i] {
			continue
		}
		return i
	}
	return -1
}

// pickWeighted runs one smooth weighted round-robin pass over the
// closed, positive-weight targets that have concurrency capacity and
// returns the selected index, or -1 when none is eligible.
func (s *Scheduler) pickWeighted() int {
	total := 0
	best := -1
	for i := range s.reqs {
		if s.state[i] != breakerClosed || s.weights[i] <= 0 {
			continue
		}
		if s.caps[i] > 0 && s.inFlight[i] >= s.caps[i] {
			continue
		}
		s.current[i] += s.weights[i]
		total += s.weights[i]
		// Strict comparison keeps the smallest index on ties, which
		// makes the generated sequence deterministic.
		if best == -1 || s.current[i] > s.current[best] {
			best = i
		}
	}
	if best == -1 {
		return -1
	}
	s.current[best] -= total
	return best
}

// waitForChange blocks the caller until a completion, Stop or the end
// of the earliest open breaker window wakes it. The caller must hold
// s.mu.
func (s *Scheduler) waitForChange() {
	var timer *time.Timer
	if d := s.nextWindowDelay(); d > 0 {
		timer = time.AfterFunc(d, func() {
			s.mu.Lock()
			s.cond.Broadcast()
			s.mu.Unlock()
		})
	}
	s.cond.Wait()
	if timer != nil {
		timer.Stop()
	}
}

// nextWindowDelay returns the time until the earliest breaker window
// of an open target ends. It returns zero when no open target is
// waiting (callers are then woken by completions alone).
func (s *Scheduler) nextWindowDelay() time.Duration {
	var earliest time.Time
	now := s.nowFunc()
	for i := range s.reqs {
		if s.state[i] != breakerOpen || s.probing[i] {
			continue
		}
		if earliest.IsZero() || s.openUntil[i].Before(earliest) {
			earliest = s.openUntil[i]
		}
	}
	if earliest.IsZero() {
		return 0
	}
	if d := earliest.Sub(now); d > 0 {
		return d
	}
	return 0
}

// Complete reports the outcome of the request dispatched to idx. A
// failed result is any transport error or HTTP response with status
// code 500 or above. It must be called exactly once for every successful
// Next call.
func (s *Scheduler) Complete(idx int, failed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.inFlight[idx]--

	switch s.state[idx] {
	case breakerHalfOpen:
		// The probe request decides whether the target recovers.
		s.probing[idx] = false
		if failed {
			s.state[idx] = breakerOpen
			s.openUntil[idx] = s.nowFunc().Add(s.window)
		} else {
			s.state[idx] = breakerClosed
			// Rejoin the rotation without carrying stale credits.
			s.current[idx] = 0
		}
	case breakerClosed:
		// Results of requests that were already in flight when the
		// circuit opened do not change its state.
		if failed && s.window > 0 {
			s.state[idx] = breakerOpen
			s.openUntil[idx] = s.nowFunc().Add(s.window)
		}
	}

	s.cond.Broadcast()
}

// Stop unblocks all Next calls and prevents further dispatches.
func (s *Scheduler) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.cond.Broadcast()
	s.mu.Unlock()
}
