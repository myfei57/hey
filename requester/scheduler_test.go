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
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestScheduler(t *testing.T, weights, caps []int, total int64, window time.Duration) *Scheduler {
	t.Helper()
	if len(weights) != len(caps) {
		t.Fatalf("weights and caps length mismatch: %d vs %d", len(weights), len(caps))
	}
	targets := make([]Target, len(weights))
	for i := range weights {
		req, err := http.NewRequest("GET", fmt.Sprintf("http://example%d.test", i), nil)
		if err != nil {
			t.Fatalf("NewRequest: %v", err)
		}
		targets[i] = Target{Request: req, Weight: weights[i], MaxConcurrent: caps[i]}
	}
	s, err := NewScheduler(targets, total, window)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	return s
}

func drain(t *testing.T, s *Scheduler) []int {
	t.Helper()
	var seq []int
	for {
		idx, ok := s.Next()
		if !ok {
			return seq
		}
		seq = append(seq, idx)
		s.Complete(idx, false)
	}
}

func TestSchedulerDeterministicSequence(t *testing.T) {
	// Weights 3:1 must produce the fixed SWRR cycle [0 0 1 0],
	// independently of how many schedulers are built.
	want := []int{0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 1, 0}
	var got1, got2 []int
	for _, got := range []*[]int{&got1, &got2} {
		s := newTestScheduler(t, []int{3, 1}, []int{0, 0}, 12, 0)
		*got = drain(t, s)
	}
	if len(got1) != 12 || len(got2) != 12 {
		t.Fatalf("expected 12 dispatches, got %d and %d", len(got1), len(got2))
	}
	for i := range want {
		if got1[i] != want[i] {
			t.Fatalf("sequence 1 mismatch at %d: got %v want %v", i, got1, want)
		}
		if got2[i] != got1[i] {
			t.Fatalf("sequence not deterministic: %v vs %v", got1, got2)
		}
	}
	counts := map[int]int{}
	for _, idx := range got1 {
		counts[idx]++
	}
	if counts[0] != 9 || counts[1] != 3 {
		t.Fatalf("expected 9/3 split, got %v", counts)
	}
}

func TestSchedulerZeroWeight(t *testing.T) {
	// A zero-weight target never receives a request.
	s := newTestScheduler(t, []int{0, 1, 2}, []int{0, 0, 0}, 12, 0)
	seq := drain(t, s)
	counts := map[int]int{}
	for _, idx := range seq {
		if idx == 0 {
			t.Fatalf("zero-weight target was dispatched: %v", seq)
		}
		counts[idx]++
	}
	if len(seq) != 12 || counts[1] != 4 || counts[2] != 8 {
		t.Fatalf("expected 4/8 split between targets 1 and 2, got %v", counts)
	}
}

func TestSchedulerValidation(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://example.test", nil)
	cases := []struct {
		name    string
		targets []Target
		total   int64
		window  time.Duration
	}{
		{"no targets", nil, 1, 0},
		{"nil request", []Target{{Request: nil, Weight: 1}}, 1, 0},
		{"negative weight", []Target{{Request: req, Weight: -1}}, 1, 0},
		{"all zero weights", []Target{{Request: req, Weight: 0}, {Request: req, Weight: 0}}, 1, 0},
		{"negative cap", []Target{{Request: req, Weight: 1, MaxConcurrent: -1}}, 1, 0},
		{"negative total", []Target{{Request: req, Weight: 1}}, -1, 0},
		{"negative window", []Target{{Request: req, Weight: 1}}, 1, -time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewScheduler(tc.targets, tc.total, tc.window); err == nil {
				t.Fatalf("expected error for %s", tc.name)
			}
		})
	}
}

func TestSchedulerBreakerLifecycle(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	s := newTestScheduler(t, []int{1, 1}, []int{0, 0}, 100, time.Minute)
	s.nowFunc = func() time.Time { return now }

	// First request goes to target 0 (smallest index on the tie).
	idx, ok := s.Next()
	if !ok || idx != 0 {
		t.Fatalf("got (%d,%v), want (0,true)", idx, ok)
	}
	s.Complete(0, true) // failure opens the circuit for one minute
	if s.state[0] != breakerOpen || !s.openUntil[0].Equal(now.Add(time.Minute)) {
		t.Fatalf("target 0 was not opened until %v: state=%d", now.Add(time.Minute), s.state[0])
	}

	// While open, all traffic shifts to target 1.
	for i := 0; i < 5; i++ {
		if idx, _ := s.Next(); idx != 1 {
			t.Fatalf("dispatch %d went to %d, want 1", i, idx)
		}
		s.Complete(1, false)
	}

	// Halfway through the window there is still no probe.
	now = now.Add(30 * time.Second)
	if idx, _ := s.Next(); idx != 1 {
		t.Fatalf("got target %d before window ended, want 1", idx)
	}
	s.Complete(1, false)

	// Once the window ends, exactly one probe is allowed to target 0.
	now = now.Add(31 * time.Second)
	if idx, _ := s.Next(); idx != 0 {
		t.Fatalf("got probe target %d, want 0", idx)
	}
	if s.state[0] != breakerHalfOpen || !s.probing[0] {
		t.Fatalf("target 0 is not half-open/probing: state=%d probing=%v", s.state[0], s.probing[0])
	}
	// A failed probe re-opens the circuit with a fresh window.
	s.Complete(0, true)
	if s.state[0] != breakerOpen || !s.openUntil[0].Equal(now.Add(time.Minute)) {
		t.Fatalf("failed probe did not re-open the circuit")
	}

	// Traffic keeps flowing to target 1 during the new window.
	if idx, _ := s.Next(); idx != 1 {
		t.Fatalf("got target %d, want 1", idx)
	}
	s.Complete(1, false)

	// Next window ends: a successful probe restores the full weight.
	now = now.Add(time.Minute)
	if idx, _ := s.Next(); idx != 0 {
		t.Fatalf("got recovery probe target %d, want 0", idx)
	}
	s.Complete(0, false)
	if s.state[0] != breakerClosed || s.probing[0] {
		t.Fatalf("target 0 did not recover: state=%d probing=%v", s.state[0], s.probing[0])
	}

	// Target 0 rejoins the rotation. Target 1 accumulated SWRR credit
	// while the circuit was open, so the deterministic sequence resumes
	// as 1,0,1,0 rather than favouring the recovered target.
	want := []int{1, 0, 1, 0}
	for i, target := range want {
		if idx, _ := s.Next(); idx != target {
			t.Fatalf("post-recovery dispatch %d got %d, want %d (full want %v)", i, idx, target, want)
		}
		s.Complete(idx, false)
	}
}

func TestSchedulerStaleResultsWhileOpen(t *testing.T) {
	now := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	// Weights 3:1 make the first two dispatches both land on target 0.
	s := newTestScheduler(t, []int{3, 1}, []int{0, 0}, 20, time.Minute)
	s.nowFunc = func() time.Time { return now }

	if idx, _ := s.Next(); idx != 0 {
		t.Fatalf("first dispatch got %d, want 0", idx)
	}
	if idx, _ := s.Next(); idx != 0 {
		t.Fatalf("second dispatch got %d, want 0", idx)
	}
	if s.inFlight[0] != 2 {
		t.Fatalf("inFlight[0]=%d, want 2", s.inFlight[0])
	}

	// One in-flight request fails and opens the circuit.
	s.Complete(0, true)
	if s.state[0] != breakerOpen {
		t.Fatalf("target 0 state=%d, want open", s.state[0])
	}
	// The stale result of the other in-flight request must not close it.
	s.Complete(0, false)
	if s.state[0] != breakerClosed && s.state[0] != breakerOpen {
		t.Fatalf("unexpected state %d", s.state[0])
	}
	if s.state[0] != breakerOpen {
		t.Fatalf("stale success closed the breaker, want it to stay open")
	}
	if s.inFlight[0] != 0 {
		t.Fatalf("inFlight[0]=%d, want 0", s.inFlight[0])
	}

	// Only target 1 is dispatchable until the window ends.
	if idx, _ := s.Next(); idx != 1 {
		t.Fatalf("got target %d, want 1", idx)
	}
	s.Complete(1, false)
}

func TestSchedulerConcurrencyCaps(t *testing.T) {
	s := newTestScheduler(t, []int{1, 1}, []int{1, 1}, 20, 0)

	var mu sync.Mutex
	inFlight := make([]int, 2)
	maxInFlight := make([]int, 2)
	dispatched := make([]int64, 2)
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx, ok := s.Next()
				if !ok {
					return
				}
				mu.Lock()
				inFlight[idx]++
				if inFlight[idx] > maxInFlight[idx] {
					maxInFlight[idx] = inFlight[idx]
				}
				mu.Unlock()
				atomic.AddInt64(&dispatched[idx], 1)
				time.Sleep(time.Millisecond)
				mu.Lock()
				inFlight[idx]--
				mu.Unlock()
				s.Complete(idx, false)
			}
		}()
	}
	wg.Wait()

	if maxInFlight[0] > 1 || maxInFlight[1] > 1 {
		t.Fatalf("per-target cap violated: max in-flight %v", maxInFlight)
	}
	if got := atomic.LoadInt64(&dispatched[0]) + atomic.LoadInt64(&dispatched[1]); got != 20 {
		t.Fatalf("dispatched %d requests, want 20", got)
	}
	if s.remaining != 0 {
		t.Fatalf("remaining=%d, want 0", s.remaining)
	}
}

func TestSchedulerSingleTargetCap(t *testing.T) {
	s := newTestScheduler(t, []int{1}, []int{1}, 6, 0)

	var mu sync.Mutex
	inFlight, maxInFlight := 0, 0
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				idx, ok := s.Next()
				if !ok {
					return
				}
				mu.Lock()
				inFlight++
				if inFlight > maxInFlight {
					maxInFlight = inFlight
				}
				mu.Unlock()
				time.Sleep(time.Millisecond)
				mu.Lock()
				inFlight--
				mu.Unlock()
				s.Complete(idx, false)
			}
		}()
	}
	wg.Wait()
	if maxInFlight != 1 {
		t.Fatalf("max in-flight %d, want 1", maxInFlight)
	}
}

func TestSchedulerZeroWeightWithCapDoesNotDeadlock(t *testing.T) {
	s := newTestScheduler(t, []int{0, 1}, []int{1, 1}, 4, 0)
	seq := drain(t, s)
	for _, idx := range seq {
		if idx != 1 {
			t.Fatalf("zero-weight target was dispatched: %v", seq)
		}
	}
	if len(seq) != 4 {
		t.Fatalf("got %d dispatches, want 4", len(seq))
	}
}

func TestSchedulerTotalBoundary(t *testing.T) {
	// More workers than requests: only one dispatch must happen.
	s := newTestScheduler(t, []int{1, 1}, []int{0, 0}, 1, 0)
	var dispatches int64
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if idx, ok := s.Next(); ok {
				atomic.AddInt64(&dispatches, 1)
				s.Complete(idx, false)
			}
		}()
	}
	wg.Wait()
	if dispatches != 1 {
		t.Fatalf("dispatched %d, want exactly 1", dispatches)
	}
	if _, ok := s.Next(); ok {
		t.Fatal("Next returned ok after the budget was exhausted")
	}
}

func TestSchedulerStopUnblocks(t *testing.T) {
	s := newTestScheduler(t, []int{1}, []int{1}, 100, 0)
	if idx, ok := s.Next(); !ok || idx != 0 {
		t.Fatalf("got (%d,%v), want (0,true)", idx, ok)
	}

	result := make(chan bool, 1)
	go func() {
		_, ok := s.Next() // blocked: the only cap slot is held
		result <- ok
	}()
	time.Sleep(20 * time.Millisecond)
	s.Stop()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("Next returned ok after Stop")
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not unblock Next")
	}
}

func TestWorkMultiTargetDeterministic(t *testing.T) {
	var counts [2]int64
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		i := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt64(&counts[i], 1)
		}))
	}
	defer servers[0].Close()
	defer servers[1].Close()

	req0, _ := http.NewRequest("GET", servers[0].URL, nil)
	req1, _ := http.NewRequest("GET", servers[1].URL, nil)
	w := &Work{
		Targets: []Target{
			{Request: req0, Weight: 3},
			{Request: req1, Weight: 1},
		},
		N:             40,
		C:             4,
		BreakerWindow: 0,
	}
	w.Run()

	if got := atomic.LoadInt64(&counts[0]); got != 30 {
		t.Errorf("target 0 received %d requests, want 30", got)
	}
	if got := atomic.LoadInt64(&counts[1]); got != 10 {
		t.Errorf("target 1 received %d requests, want 10", got)
	}
}

func TestWorkMultiTargetBreakerDiversionAndRecovery(t *testing.T) {
	var aCount, bCount, bSuccess, bFailures int64
	serverA := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&aCount, 1)
		// Keep the run comfortably longer than several breaker windows
		// so window expiry and recovery probes are guaranteed to happen.
		time.Sleep(time.Millisecond)
	}))
	serverB := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&bCount, 1)
		time.Sleep(time.Millisecond)
		if atomic.AddInt64(&bFailures, 1) <= 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		atomic.AddInt64(&bSuccess, 1)
	}))
	defer serverA.Close()
	defer serverB.Close()

	reqA, _ := http.NewRequest("GET", serverA.URL, nil)
	reqB, _ := http.NewRequest("GET", serverB.URL, nil)
	w := &Work{
		Targets: []Target{
			{Request: reqA, Weight: 1},
			{Request: reqB, Weight: 1},
		},
		N:             300,
		C:             2,
		BreakerWindow: 15 * time.Millisecond,
		Timeout:       20,
	}
	w.Run()

	if got := atomic.LoadInt64(&aCount) + atomic.LoadInt64(&bCount); got != 300 {
		t.Errorf("total requests = %d, want 300", got)
	}
	if atomic.LoadInt64(&bFailures) < 1 {
		t.Error("target B never recorded a failure")
	}
	if atomic.LoadInt64(&bSuccess) < 1 {
		t.Error("target B never recovered via a successful probe")
	}
	if atomic.LoadInt64(&aCount) <= atomic.LoadInt64(&bCount) {
		t.Errorf("traffic was not diverted to A: A=%d B=%d", aCount, bCount)
	}
}

func TestWorkAllTargetsOpenThenRecover(t *testing.T) {
	var failures [2]int64
	var successes [2]int64
	servers := make([]*httptest.Server, 2)
	for i := range servers {
		i := i
		servers[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if atomic.AddInt64(&failures[i], 1) <= 2 {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			atomic.AddInt64(&successes[i], 1)
		}))
	}
	defer servers[0].Close()
	defer servers[1].Close()

	req0, _ := http.NewRequest("GET", servers[0].URL, nil)
	req1, _ := http.NewRequest("GET", servers[1].URL, nil)
	w := &Work{
		Targets: []Target{
			{Request: req0, Weight: 1},
			{Request: req1, Weight: 1},
		},
		N:             40,
		C:             4,
		BreakerWindow: 25 * time.Millisecond,
		Timeout:       20,
	}
	done := make(chan struct{})
	go func() {
		w.Run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deadlock: open breaker windows were never probed after they ended")
	}
	for i := range successes {
		if atomic.LoadInt64(&successes[i]) < 1 {
			t.Errorf("target %d never recovered", i)
		}
	}
}
