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
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeNetError struct {
	timeout bool
}

func (e fakeNetError) Error() string   { return "fake network error" }
func (e fakeNetError) Timeout() bool   { return e.timeout }
func (e fakeNetError) Temporary() bool { return false }

func TestClassifyResult(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		statusCode int
		want       LatencyClass
	}{
		{"success", nil, 200, LatencySuccess},
		{"redirect is success", nil, 301, LatencySuccess},
		{"bad request", nil, 400, LatencyHTTPError},
		{"not found", nil, 404, LatencyHTTPError},
		{"server error", nil, 503, LatencyHTTPError},
		{"plain error", errors.New("boom"), 0, LatencyNetworkError},
		{"wrapped url error", &url.Error{Op: "Get", URL: "http://x", Err: errors.New("connection refused")}, 0, LatencyNetworkError},
		{"context deadline", context.DeadlineExceeded, 0, LatencyTimeout},
		{"wrapped deadline", &url.Error{Op: "Get", URL: "http://x", Err: context.DeadlineExceeded}, 0, LatencyTimeout},
		{"net timeout", fakeNetError{timeout: true}, 0, LatencyTimeout},
		{"net non-timeout", fakeNetError{timeout: false}, 0, LatencyNetworkError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyResult(tc.err, tc.statusCode); got != tc.want {
				t.Errorf("ClassifyResult(%v, %d) = %v, want %v", tc.err, tc.statusCode, got, tc.want)
			}
		})
	}
}

func TestLatencyClassStringStable(t *testing.T) {
	want := map[LatencyClass]string{
		LatencySuccess:      "success",
		LatencyHTTPError:    "http-error",
		LatencyNetworkError: "network-error",
		LatencyTimeout:      "timeout",
	}
	for class, name := range want {
		if got := class.String(); got != name {
			t.Errorf("class %d String() = %q, want %q", class, got, name)
		}
	}
}

func TestBucketIndexAndBounds(t *testing.T) {
	const cap = 64
	cases := []struct {
		ns      int64
		wantIdx int
		wantLo  int64
		wantHi  int64
	}{
		{0, 0, 0, 1},
		{1, 1, 1, 2},
		{7, 7, 7, 8},
		{8, 8, 8, 9},
		{15, 15, 15, 16},
		{16, 16, 16, 18},
		{17, 16, 16, 18},
		{30, 23, 30, 32},
		{31, 23, 30, 32},
		{32, 24, 32, 36},
	}
	for _, tc := range cases {
		idx := bucketIndex(tc.ns, cap)
		if idx != tc.wantIdx {
			t.Errorf("bucketIndex(%d) = %d, want %d", tc.ns, idx, tc.wantIdx)
		}
		lo, hi := bucketBounds(idx)
		if lo != tc.wantLo || hi != tc.wantHi {
			t.Errorf("bucketBounds(%d) = (%d, %d), want (%d, %d)", idx, lo, hi, tc.wantLo, tc.wantHi)
		}
		if lo > tc.ns || tc.ns >= hi {
			t.Errorf("value %d not contained in its bin bounds [%d, %d)", tc.ns, lo, hi)
		}
	}
	// Values beyond the cap saturate into the final bin.
	if got := bucketIndex(1<<40, cap); got != cap-1 {
		t.Errorf("bucketIndex(1<<40) = %d, want saturation at %d", got, cap-1)
	}
}

func TestLatencySummaryQuantileDeterministic(t *testing.T) {
	a := NewLatencyAggregator(defaultLatencyBuckets)
	// Empty summary: every quantile is zero.
	if got := a.summaries[LatencySuccess].quantile(0.5); got != 0 {
		t.Errorf("quantile on empty summary = %v, want 0", got)
	}

	const observations = 100
	d := time.Millisecond
	for i := 0; i < observations; i++ {
		a.Observe(LatencySuccess, d)
	}
	s := a.summaries[LatencySuccess]
	if s.count != observations {
		t.Fatalf("count = %d, want %d", s.count, observations)
	}
	if s.minNs != d.Nanoseconds() || s.maxNs != d.Nanoseconds() {
		t.Errorf("exact min/max lost: min=%d max=%d, want %d", s.minNs, s.maxNs, d.Nanoseconds())
	}
	if got := s.quantile(0); got != s.quantile(0) {
		t.Fatal("quantile output is not stable")
	}
	p50 := s.quantile(0.5)
	p100 := s.quantile(1)
	// Deterministic: repeated computation must return identical values.
	if p50 != s.quantile(0.5) || p100 != s.quantile(1) {
		t.Fatal("quantile output is not deterministic across calls")
	}
	// The estimate must stay inside the bin covering 1ms (within ~10%).
	if p50 <= 0 || p50 < 0.0009 || p50 > 0.0011 {
		t.Errorf("p50 = %v secs, want ~0.001", p50)
	}
	if p100 < p50 {
		t.Errorf("p100 (%v) must be >= p50 (%v)", p100, p50)
	}

	// A second aggregator over the same input must produce identical stats.
	b := NewLatencyAggregator(defaultLatencyBuckets)
	for i := 0; i < observations; i++ {
		b.Observe(LatencySuccess, d)
	}
	if !reflect.DeepEqual(a.Stats(nil), b.Stats(nil)) {
		t.Fatal("identical input streams produced different stats")
	}
}

func TestNormalizePercentages(t *testing.T) {
	got := normalizePercentages([]int{99, 50, -1, 101, 50, 10, 10})
	want := []int{10, 50, 99}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("normalizePercentages = %v, want %v", got, want)
	}
	if got := normalizePercentages(nil); !reflect.DeepEqual(got, DefaultPercentiles) {
		t.Errorf("default percentages = %v, want %v", got, DefaultPercentiles)
	}
}

func TestLatencyAggregatorMergeEquivalence(t *testing.T) {
	values := make([]time.Duration, 0, 200)
	for i := 1; i <= 200; i++ {
		values = append(values, time.Duration(((i*37)%5000)+1)*time.Microsecond)
	}
	single := NewLatencyAggregator(defaultLatencyBuckets)
	left := NewLatencyAggregator(defaultLatencyBuckets)
	right := NewLatencyAggregator(defaultLatencyBuckets)
	for i, v := range values {
		class := LatencyClass(i % int(latencyClassCount))
		single.Observe(class, v)
		if i%2 == 0 {
			left.Observe(class, v)
		} else {
			right.Observe(class, v)
		}
	}
	if err := left.Merge(right); err != nil {
		t.Fatalf("Merge failed: %v", err)
	}
	if !reflect.DeepEqual(left.summaries, single.summaries) {
		t.Fatal("merged summaries differ from the single-stream summary")
	}
	if !reflect.DeepEqual(left.Stats(DefaultPercentiles), single.Stats(DefaultPercentiles)) {
		t.Fatal("merged stats differ from the single-stream stats")
	}

	// Merging aggregators with incompatible caps must be rejected.
	big := NewLatencyAggregator(128)
	if err := single.Merge(big); err == nil {
		t.Fatal("expected error when merging aggregators with different caps")
	}
}

func TestLatencyAggregatorBoundedMemory(t *testing.T) {
	cap := 256
	a := NewLatencyAggregator(cap)
	if a.BucketCap() != cap {
		t.Fatalf("BucketCap = %d, want %d", a.BucketCap(), cap)
	}
	const total = 200000
	for i := 0; i < total; i++ {
		class := LatencyClass(i % int(latencyClassCount))
		a.Observe(class, time.Duration((i%5000)+1)*time.Microsecond)
	}
	var seen int64
	for class := LatencyClass(0); int(class) < int(latencyClassCount); class++ {
		s := a.summaries[class]
		if len(s.bins) != cap {
			t.Errorf("class %d bins grew to %d, want fixed cap %d", class, len(s.bins), cap)
		}
		seen += s.count
	}
	if seen != total {
		t.Errorf("total observed = %d, want %d", seen, total)
	}

	// Constructor clamps caller-supplied caps to keep memory bounded.
	if NewLatencyAggregator(0).BucketCap() != defaultLatencyBuckets {
		t.Error("zero cap should fall back to the default")
	}
	if NewLatencyAggregator(2).BucketCap() != defaultLatencyBuckets {
		t.Error("sub-octave cap should fall back to the default")
	}
	if got := NewLatencyAggregator(maxLatencyBuckets + 10).BucketCap(); got != maxLatencyBuckets {
		t.Errorf("oversized cap = %d, want clamp to %d", got, maxLatencyBuckets)
	}
}

func TestLatencySummaryStoresNoRawSamples(t *testing.T) {
	typ := reflect.TypeOf(latencySummary{})
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Type.Kind() == reflect.Slice && field.Name != "bins" {
			t.Errorf("latencySummary retains raw samples in slice field %q", field.Name)
		}
	}
	a := NewLatencyAggregator(32)
	for i := 0; i < 1000; i++ {
		a.Observe(LatencySuccess, time.Duration(i+1)*time.Nanosecond)
	}
	if len(a.summaries[LatencySuccess].bins) != 32 {
		t.Errorf("bins length = %d, want fixed 32", len(a.summaries[LatencySuccess].bins))
	}
}

func TestLatencyAggregatorConcurrent(t *testing.T) {
	a := NewLatencyAggregator(defaultLatencyBuckets)
	const goroutines, perGoroutine = 64, 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				class := LatencyClass((g + i) % int(latencyClassCount))
				a.Observe(class, time.Duration(g*perGoroutine+i+1)*time.Microsecond)
				// Concurrent reads must be safe alongside writes.
				_ = a.Stats(DefaultPercentiles)
			}
		}(g)
	}
	wg.Wait()
	wantPerClass := int64(goroutines * perGoroutine / int(latencyClassCount))
	for class := LatencyClass(0); int(class) < int(latencyClassCount); class++ {
		if got := a.summaries[class].count; got != wantPerClass {
			t.Errorf("class %d count = %d, want %d", class, got, wantPerClass)
		}
	}
}

func TestLatencyAggregatorSnapshotIndependent(t *testing.T) {
	a := NewLatencyAggregator(defaultLatencyBuckets)
	a.Observe(LatencySuccess, time.Millisecond)
	snap := a.Snapshot()
	a.Observe(LatencySuccess, time.Millisecond)
	if snap.summaries[LatencySuccess].count != 1 {
		t.Errorf("snapshot count = %d, want 1 (must not observe later writes)", snap.summaries[LatencySuccess].count)
	}
	if a.summaries[LatencySuccess].count != 2 {
		t.Errorf("original count = %d, want 2", a.summaries[LatencySuccess].count)
	}
	if &snap.summaries[LatencySuccess].bins[0] == &a.summaries[LatencySuccess].bins[0] {
		t.Error("snapshot shares bin storage with the live aggregator")
	}
}

func TestWorkLatencyStatsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	buf := &bytes.Buffer{}
	w := &Work{Request: req, N: 20, C: 2, Writer: buf}
	w.Run()

	stats := w.LatencyStats(nil)
	if len(stats) != int(latencyClassCount) {
		t.Fatalf("stats entries = %d, want %d", len(stats), latencyClassCount)
	}
	for i, st := range stats {
		if st.Class != LatencyClass(i) {
			t.Errorf("stats[%d].Class = %v, want fixed class order %v", i, st.Class, LatencyClass(i))
		}
	}
	if got := stats[LatencySuccess].Count; got != 20 {
		t.Errorf("success count = %d, want 20", got)
	}
	for class := LatencyHTTPError; class <= LatencyTimeout; class++ {
		if stats[class].Count != 0 {
			t.Errorf("class %v count = %d, want 0", class, stats[class].Count)
		}
		if stats[class].Min != 0 || stats[class].Max != 0 || stats[class].Mean != 0 {
			t.Errorf("empty class %v should report zero extrema", class)
		}
	}
	if len(stats[LatencySuccess].Percentiles) != len(DefaultPercentiles) {
		t.Errorf("percentile count = %d, want %d", len(stats[LatencySuccess].Percentiles), len(DefaultPercentiles))
	}
	// Deterministic end-to-end output.
	if !reflect.DeepEqual(w.LatencyStats(nil), stats) {
		t.Fatal("LatencyStats is not deterministic across calls")
	}
	// The existing report output is still produced and untouched.
	if !strings.Contains(buf.String(), "Summary:") || !strings.Contains(buf.String(), "Latency distribution:") {
		t.Errorf("existing summary report missing from output:\n%s", buf.String())
	}
}

func TestWorkLatencyStatsHTTPError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	buf := &bytes.Buffer{}
	w := &Work{Request: req, N: 10, C: 2, Writer: buf}
	w.Run()

	stats := w.LatencyStats(nil)
	if got := stats[LatencyHTTPError].Count; got != 10 {
		t.Errorf("http-error count = %d, want 10", got)
	}
	if stats[LatencySuccess].Count != 0 {
		t.Errorf("success count = %d, want 0", stats[LatencySuccess].Count)
	}
	if !strings.Contains(buf.String(), "Status code distribution:") {
		t.Errorf("report output changed, status distribution missing:\n%s", buf.String())
	}
}

func TestWorkLatencyStatsTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
	}))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	buf := &bytes.Buffer{}
	w := &Work{Request: req, N: 3, C: 3, Timeout: 1, Writer: buf}
	w.Run()

	stats := w.LatencyStats(nil)
	if got := stats[LatencyTimeout].Count; got != 3 {
		t.Errorf("timeout count = %d, want 3", got)
	}
	if stats[LatencyNetworkError].Count != 0 || stats[LatencySuccess].Count != 0 {
		t.Errorf("unexpected classification: %+v", stats)
	}
}

func TestWorkLatencyStatsNetworkError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	buf := &bytes.Buffer{}
	w := &Work{Request: req, N: 6, C: 2, Timeout: 5, Writer: buf}
	w.Run()

	stats := w.LatencyStats(nil)
	if got := stats[LatencyNetworkError].Count; got != 6 {
		t.Errorf("network-error count = %d, want 6", got)
	}
	if stats[LatencyTimeout].Count != 0 {
		t.Errorf("timeout count = %d, want 0", stats[LatencyTimeout].Count)
	}
}

func TestWorkReportOutputUnchangedCSV(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	defer server.Close()

	req, _ := http.NewRequest("GET", server.URL, nil)
	buf := &bytes.Buffer{}
	w := &Work{Request: req, N: 4, C: 1, Output: "csv", Writer: buf}
	w.Run()

	wantHeader := "response-time,DNS+dialup,DNS,Request-write,Response-delay,Response-read,status-code,offset"
	if !strings.Contains(buf.String(), wantHeader) {
		t.Errorf("CSV header missing or altered, output:\n%s", buf.String())
	}
	if lines := strings.Count(strings.TrimSpace(buf.String()), "\n") + 1; lines != 1+4 {
		t.Errorf("CSV data rows = %d, want header + 4 rows", lines-1)
	}
}
