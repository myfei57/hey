// Copyright 2014 Google Inc. All Rights Reserved.
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
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// waitUntil polls cond until it is true or the timeout elapses.
func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// assertSchedulerInvariants checks the accounting identities of a snapshot.
func assertSchedulerInvariants(t *testing.T, s SchedulerSnapshot) {
	t.Helper()
	if s.Sent != s.InFlight+s.Succeeded+s.Failed {
		t.Errorf("Sent(%d) != InFlight(%d)+Succeeded(%d)+Failed(%d)",
			s.Sent, s.InFlight, s.Succeeded, s.Failed)
	}
	if s.Total != s.Sent+s.Remaining {
		t.Errorf("Total(%d) != Sent(%d)+Remaining(%d)", s.Total, s.Sent, s.Remaining)
	}
	if s.Sent < 0 || s.InFlight < 0 || s.Succeeded < 0 || s.Failed < 0 || s.Remaining < 0 {
		t.Errorf("snapshot contains negative counters: %+v", s)
	}
}

func TestN(t *testing.T) {
	var count int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&count, int64(1))
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	w := &Work{
		Request: req,
		N:       20,
		C:       2,
	}
	w.Run()
	if count != 20 {
		t.Errorf("Expected to send 20 requests, found %v", count)
	}
}

func TestQps(t *testing.T) {
	var wg sync.WaitGroup
	var count int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&count, int64(1))
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	w := &Work{
		Request: req,
		N:       20,
		C:       2,
		QPS:     1,
	}
	wg.Add(1)
	time.AfterFunc(time.Second, func() {
		if count > 2 {
			t.Errorf("Expected to work at most 2 times, found %v", count)
		}
		wg.Done()
	})
	go w.Run()
	wg.Wait()
}

func TestRequest(t *testing.T) {
	var uri, contentType, some, auth string
	handler := func(w http.ResponseWriter, r *http.Request) {
		uri = r.RequestURI
		contentType = r.Header.Get("Content-type")
		some = r.Header.Get("X-some")
		auth = r.Header.Get("Authorization")
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	header := make(http.Header)
	header.Add("Content-type", "text/html")
	header.Add("X-some", "value")
	req, _ := http.NewRequest("GET", server.URL, nil)
	req.Header = header
	req.SetBasicAuth("username", "password")
	w := &Work{
		Request: req,
		N:       1,
		C:       1,
	}
	w.Run()
	if uri != "/" {
		t.Errorf("Uri is expected to be /, %v is found", uri)
	}
	if contentType != "text/html" {
		t.Errorf("Content type is expected to be text/html, %v is found", contentType)
	}
	if some != "value" {
		t.Errorf("X-some header is expected to be value, %v is found", some)
	}
	if auth != "Basic dXNlcm5hbWU6cGFzc3dvcmQ=" {
		t.Errorf("Basic authorization is not properly set")
	}
}

func TestBody(t *testing.T) {
	var count int64
	handler := func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) == "Body" {
			atomic.AddInt64(&count, 1)
		}
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	req, _ := http.NewRequest("POST", server.URL, bytes.NewBuffer([]byte("Body")))
	w := &Work{
		Request:     req,
		RequestBody: []byte("Body"),
		N:           10,
		C:           1,
	}
	w.Run()
	if count != 10 {
		t.Errorf("Expected to work 10 times, found %v", count)
	}
}

func TestSchedulerSnapshotPauseResume(t *testing.T) {
	const n, c = 12, 4
	var hits int64
	release := make(chan struct{})
	handler := func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
		<-release
	}
	server := httptest.NewServer(http.HandlerFunc(handler))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	w := &Work{Request: req, N: n, C: c}
	w.Init()

	done := make(chan struct{})
	go func() {
		w.Run()
		close(done)
	}()

	// Wait until every worker is busy with an in-flight request.
	waitUntil(t, 5*time.Second, func() bool {
		return atomic.LoadInt64(&hits) >= c
	})

	w.Pause()
	if snap := w.Snapshot(); !snap.Paused {
		t.Fatal("snapshot taken after Pause must report Paused=true")
	}

	// In-flight requests must be allowed to finish while paused.
	close(release)
	waitUntil(t, 5*time.Second, func() bool {
		return w.Snapshot().InFlight == 0
	})

	pausedSnap := w.Snapshot()
	assertSchedulerInvariants(t, pausedSnap)
	if !pausedSnap.Paused || pausedSnap.InFlight != 0 {
		t.Fatalf("unexpected paused snapshot: %+v", pausedSnap)
	}
	if pausedSnap.Sent != pausedSnap.Succeeded+pausedSnap.Failed {
		t.Fatalf("drained snapshot must classify all sent requests: %+v", pausedSnap)
	}
	if pausedSnap.Sent >= n || pausedSnap.Remaining != n-pausedSnap.Sent {
		t.Fatalf("paused run should still have requests left: %+v", pausedSnap)
	}

	// No new requests may be dispatched while paused.
	frozen := pausedSnap.Sent
	time.Sleep(100 * time.Millisecond)
	if s := w.Snapshot(); s.Sent != frozen {
		t.Fatalf("scheduler dispatched %d requests while paused", s.Sent-frozen)
	}
	if got := atomic.LoadInt64(&hits); got != frozen {
		t.Fatalf("server received %d hits while paused, want %d", got, frozen)
	}

	// Resume must validate the parameter version.
	if _, err := w.Resume(pausedSnap.ParamsVersion + 1); !errors.Is(err, ErrParamsVersionMismatch) {
		t.Fatalf("Resume with wrong version: expected ErrParamsVersionMismatch, got %v", err)
	}
	if s := w.Snapshot(); !s.Paused {
		t.Fatal("scheduler must stay paused after a rejected resume")
	}
	time.Sleep(50 * time.Millisecond)
	if got := atomic.LoadInt64(&hits); got != frozen {
		t.Fatalf("rejected resume dispatched requests: hits=%d want %d", got, frozen)
	}

	resumed, err := w.Resume(pausedSnap.ParamsVersion)
	if err != nil {
		t.Fatalf("Resume with the snapshot version failed: %v", err)
	}
	if resumed.Paused {
		t.Fatal("snapshot returned by successful Resume must report Paused=false")
	}

	<-done

	final := w.Snapshot()
	assertSchedulerInvariants(t, final)
	if final.Sent != n || final.Remaining != 0 || final.InFlight != 0 {
		t.Errorf("unexpected final snapshot: %+v", final)
	}
	if final.Succeeded != n || final.Failed != 0 {
		t.Errorf("expected %d successes and 0 failure, got %+v", n, final)
	}
	if got := atomic.LoadInt64(&hits); got != n {
		t.Errorf("each request must be sent exactly once: hits=%d want %d", got, n)
	}
}

func TestPauseResumeCyclesAtMostOnce(t *testing.T) {
	const n, c = 40, 8
	var hits int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
	}))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	w := &Work{Request: req, N: n, C: c}
	w.Init()

	done := make(chan struct{})
	go func() {
		w.Run()
		close(done)
	}()

	for i := 0; i < 3; i++ {
		waitUntil(t, 5*time.Second, func() bool {
			return atomic.LoadInt64(&hits) > 0
		})
		w.Pause()
		// Pause is cooperative: workers that already passed the gate finish
		// their in-flight request. Wait for the scheduler to quiesce, after
		// which no further request may be dispatched.
		waitUntil(t, 5*time.Second, func() bool {
			first := w.Snapshot()
			time.Sleep(20 * time.Millisecond)
			second := w.Snapshot()
			return first.InFlight == 0 && first.Sent == second.Sent
		})
		before := w.Snapshot().Sent
		if got := atomic.LoadInt64(&hits); got != before {
			t.Fatalf("cycle %d: server hits(%d) do not match dispatched count(%d)", i, got, before)
		}
		time.Sleep(50 * time.Millisecond)
		if s := w.Snapshot(); s.Sent != before {
			t.Fatalf("cycle %d: scheduler dispatched %d requests while paused", i, s.Sent-before)
		}
		snap := w.Snapshot()
		assertSchedulerInvariants(t, snap)
		if _, err := w.Resume(snap.ParamsVersion); err != nil {
			t.Fatalf("cycle %d resume failed: %v", i, err)
		}
	}

	<-done

	final := w.Snapshot()
	assertSchedulerInvariants(t, final)
	if final.Sent != n || final.Remaining != 0 {
		t.Errorf("unexpected final snapshot: %+v", final)
	}
	if got := atomic.LoadInt64(&hits); got != n {
		t.Errorf("each request must be sent at most once across pause cycles: hits=%d want %d", got, n)
	}
}

func TestSnapshotFailedCounter(t *testing.T) {
	const n, c = 10, 2
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := server.URL
	server.Close() // every request must now fail

	req, _ := http.NewRequest("GET", url, nil)
	w := &Work{Request: req, N: n, C: c}
	w.Run()

	final := w.Snapshot()
	assertSchedulerInvariants(t, final)
	if final.Sent != n || final.Remaining != 0 || final.InFlight != 0 {
		t.Errorf("unexpected final snapshot: %+v", final)
	}
	if final.Failed != n || final.Succeeded != 0 {
		t.Errorf("expected %d failures and 0 success, got %+v", n, final)
	}
}

func TestSnapshotConcurrentDoesNotBlock(t *testing.T) {
	const n, c = 200, 16
	var hits int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&hits, 1)
	}))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	w := &Work{Request: req, N: n, C: c}
	w.Init()

	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				assertSchedulerInvariants(t, w.Snapshot())
			}
		}
	}()

	w.Run()
	close(stop)

	final := w.Snapshot()
	assertSchedulerInvariants(t, final)
	if final.Sent != n || final.Remaining != 0 {
		t.Errorf("unexpected final snapshot: %+v", final)
	}
	if got := atomic.LoadInt64(&hits); got != n {
		t.Errorf("hits=%d want %d", got, n)
	}
}

func TestPauseResumeIdempotent(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	w := &Work{Request: req, N: 10, C: 2}
	w.Init()

	w.Pause()
	w.Pause() // idempotent
	if s := w.Snapshot(); !s.Paused {
		t.Fatal("double Pause must keep the scheduler paused")
	}

	snap, err := w.Resume(w.ParamsVersion)
	if err != nil {
		t.Fatalf("Resume failed: %v", err)
	}
	if snap.Paused {
		t.Fatal("Resume must clear the paused flag")
	}
	// Resuming a running (not paused) Work is a no-op that still validates
	// the parameter version.
	if _, err := w.Resume(w.ParamsVersion); err != nil {
		t.Fatalf("second Resume failed: %v", err)
	}
	if _, err := w.Resume(w.ParamsVersion + 1); !errors.Is(err, ErrParamsVersionMismatch) {
		t.Fatalf("wrong version: expected ErrParamsVersionMismatch, got %v", err)
	}
}

func TestComputeParamsVersion(t *testing.T) {
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	w1 := &Work{Request: req, N: 10, C: 2, QPS: 1, Timeout: 5}
	w1.Init()
	w2 := &Work{Request: req.Clone(req.Context()), N: 10, C: 2, QPS: 1, Timeout: 5}
	w2.Init()
	if w1.ParamsVersion == 0 || w1.ParamsVersion != w2.ParamsVersion {
		t.Fatalf("identical parameters must share a version: %d vs %d",
			w1.ParamsVersion, w2.ParamsVersion)
	}

	w3 := &Work{Request: req.Clone(req.Context()), N: 20, C: 2, QPS: 1, Timeout: 5}
	w3.Init()
	if w3.ParamsVersion == w1.ParamsVersion {
		t.Fatal("changing N must change the parameter version")
	}

	w4 := &Work{Request: req.Clone(req.Context()), N: 10, C: 2, ParamsVersion: 42}
	w4.Init()
	if w4.ParamsVersion != 42 {
		t.Fatalf("explicit ParamsVersion must be preserved, got %d", w4.ParamsVersion)
	}
}
