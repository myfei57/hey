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
	"context"
	"errors"
	"math"
	"math/bits"
	"net"
	"os"
	"sort"
	"sync"
	"time"
)

// LatencyClass identifies how a request finished. Every observation is
// routed to exactly one class and each class keeps an independent summary.
type LatencyClass int

const (
	// LatencySuccess is a completed transport with a status code below 400.
	LatencySuccess LatencyClass = iota
	// LatencyHTTPError is a completed transport with a status code of 400+.
	LatencyHTTPError
	// LatencyNetworkError is a transport error that is not a timeout.
	LatencyNetworkError
	// LatencyTimeout is a transport error caused by a deadline/timeout.
	LatencyTimeout

	// latencyClassCount is the number of tracked classes. It must stay last.
	latencyClassCount
)

// String returns a stable, human-readable name for the class.
func (c LatencyClass) String() string {
	switch c {
	case LatencySuccess:
		return "success"
	case LatencyHTTPError:
		return "http-error"
	case LatencyNetworkError:
		return "network-error"
	case LatencyTimeout:
		return "timeout"
	default:
		return "unknown"
	}
}

const (
	// subBucketBits is the number of mantissa bits kept per power-of-two
	// octave, giving 2^subBucketBits equally spaced sub-buckets per octave.
	subBucketBits = 3
	subBucketCnt  = 1 << subBucketBits
	subBucketMask = subBucketCnt - 1

	// defaultLatencyBuckets is the default fixed number of bins per class.
	// With 8 sub-buckets per octave it covers latencies up to roughly
	// 2^49 nanoseconds (about 13 days); larger values saturate the last bin.
	defaultLatencyBuckets = 384
	// maxLatencyBuckets bounds caller-supplied bucket counts so memory use
	// stays bounded even when a caller asks for a huge cap.
	maxLatencyBuckets = 4096
)

// DefaultPercentiles are the quantiles reported by the existing report.
var DefaultPercentiles = []int{10, 25, 50, 75, 90, 95, 99}

// LatencyPercentile is a single deterministic percentile estimate.
// Latency is expressed in seconds.
type LatencyPercentile struct {
	Percentage int
	Latency    float64
}

// LatencyClassStats is the immutable view of one class summary.
type LatencyClassStats struct {
	Class LatencyClass
	Count int64
	// Min, Max and Mean are exact values in seconds derived from observations.
	Min  float64
	Max  float64
	Mean float64
	// Percentiles are ordered by ascending percentage. Empty classes report
	// zero latencies for every requested percentage.
	Percentiles []LatencyPercentile
}

// latencySummary is a mergeable, bounded-memory summary for one class.
// It never stores individual observations: only counters, exact extrema and
// a fixed-size log-spaced histogram. All fields except the bins are exact;
// the histogram trades a bounded, configurable relative error for a hard
// memory cap.
type latencySummary struct {
	count int64
	sum   float64 // seconds
	minNs int64
	maxNs int64
	// bins has a fixed length for the lifetime of the summary. Observe never
	// appends to it, so memory cannot grow with the number of requests.
	bins []uint64
}

func newLatencySummary(bucketCap int) *latencySummary {
	return &latencySummary{
		minNs: math.MaxInt64,
		bins:  make([]uint64, bucketCap),
	}
}

// observe records one latency in nanoseconds. It allocates nothing.
func (s *latencySummary) observe(ns int64) {
	s.count++
	secs := float64(ns) / float64(time.Second)
	s.sum += secs
	if ns < s.minNs {
		s.minNs = ns
	}
	if ns > s.maxNs {
		s.maxNs = ns
	}
	idx := bucketIndex(ns, len(s.bins))
	// Saturate instead of wrapping so a pathologically long run cannot
	// corrupt cumulative counts used for quantile lookup.
	if s.bins[idx] < math.MaxUint64 {
		s.bins[idx]++
	}
}

// merge folds another summary with the same bucket layout into s.
func (s *latencySummary) merge(other *latencySummary) {
	if other == nil || other.count == 0 {
		return
	}
	s.count += other.count
	s.sum += other.sum
	if other.minNs < s.minNs {
		s.minNs = other.minNs
	}
	if other.maxNs > s.maxNs {
		s.maxNs = other.maxNs
	}
	for i := range s.bins {
		// Defensive: summaries merged here always share a cap, but never
		// read past the shorter bin slice.
		if i >= len(other.bins) {
			break
		}
		ov := other.bins[i]
		if ov == 0 {
			continue
		}
		if math.MaxUint64-s.bins[i] < ov {
			s.bins[i] = math.MaxUint64
		} else {
			s.bins[i] += ov
		}
	}
}

func (s *latencySummary) clone() *latencySummary {
	cp := &latencySummary{
		count: s.count,
		sum:   s.sum,
		minNs: s.minNs,
		maxNs: s.maxNs,
		bins:  make([]uint64, len(s.bins)),
	}
	copy(cp.bins, s.bins)
	return cp
}

// quantile returns the nearest-rank estimate for q in [0, 1] in seconds.
// The result is deterministic: it depends solely on bin counts and fixed
// bin edges, never on observation order or random sampling.
func (s *latencySummary) quantile(q float64) float64 {
	if s.count == 0 {
		return 0
	}
	if q <= 0 {
		q = 0
	}
	if q >= 1 {
		q = 1
	}
	target := int64(math.Ceil(q * float64(s.count)))
	if target < 1 {
		target = 1
	}
	if target > s.count {
		target = s.count
	}
	var cumulative int64
	for i, n := range s.bins {
		if n == 0 {
			continue
		}
		cumulative += int64(n)
		if cumulative >= target {
			lo, hi := bucketBounds(i)
			// Geometric midpoint matches the log-spaced bin layout and is
			// zero for the bucket holding only zero-length observations.
			return math.Sqrt(float64(lo)*float64(hi)) / float64(time.Second)
		}
	}
	return 0
}

func (s *latencySummary) stats(class LatencyClass, pcts []int) LatencyClassStats {
	out := LatencyClassStats{
		Class:       class,
		Count:       s.count,
		Percentiles: make([]LatencyPercentile, 0, len(pcts)),
	}
	if s.count > 0 {
		out.Min = float64(s.minNs) / float64(time.Second)
		out.Max = float64(s.maxNs) / float64(time.Second)
		out.Mean = s.sum / float64(s.count)
	}
	for _, p := range pcts {
		out.Percentiles = append(out.Percentiles, LatencyPercentile{
			Percentage: p,
			Latency:    s.quantile(float64(p) / 100),
		})
	}
	return out
}

// LatencyAggregator maintains one bounded, mergeable latency summary per
// LatencyClass. It is safe for concurrent use: worker goroutines may call
// Observe concurrently while another goroutine later reads Stats or Merge.
type LatencyAggregator struct {
	mu        sync.RWMutex
	bucketCap int
	summaries [latencyClassCount]*latencySummary
}

// NewLatencyAggregator creates an aggregator whose per-class summaries hold
// at most bucketCap bins. A non-positive or undersized value falls back to
// defaultLatencyBuckets and values above maxLatencyBuckets are clamped, so
// total memory is always bounded by 4*bucketCap*8 bytes plus a few scalars.
func NewLatencyAggregator(bucketCap int) *LatencyAggregator {
	bucketCap = normalizeBucketCap(bucketCap)
	a := &LatencyAggregator{bucketCap: bucketCap}
	for i := range a.summaries {
		a.summaries[i] = newLatencySummary(bucketCap)
	}
	return a
}

func normalizeBucketCap(bucketCap int) int {
	if bucketCap < subBucketCnt {
		return defaultLatencyBuckets
	}
	if bucketCap > maxLatencyBuckets {
		return maxLatencyBuckets
	}
	return bucketCap
}

// BucketCap returns the fixed number of bins allocated per class.
func (a *LatencyAggregator) BucketCap() int {
	return a.bucketCap
}

// ClassifyResult maps a request outcome to a LatencyClass. A non-nil error
// is either a timeout (context/network deadline) or a generic network error;
// otherwise status codes of 400 and above are HTTP errors.
func ClassifyResult(err error, statusCode int) LatencyClass {
	if err != nil {
		if isTimeoutError(err) {
			return LatencyTimeout
		}
		return LatencyNetworkError
	}
	if statusCode >= 400 {
		return LatencyHTTPError
	}
	return LatencySuccess
}

func isTimeoutError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

// Observe records d under the given class. It is safe for concurrent use and
// never retains the observation beyond its histogram counters.
func (a *LatencyAggregator) Observe(class LatencyClass, d time.Duration) {
	if a == nil || class < 0 || int(class) >= len(a.summaries) {
		return
	}
	ns := d.Nanoseconds()
	if ns < 0 {
		ns = 0
	}
	a.mu.Lock()
	a.summaries[class].observe(ns)
	a.mu.Unlock()
}

// ObserveResult classifies the request outcome and records its duration.
func (a *LatencyAggregator) ObserveResult(err error, statusCode int, d time.Duration) {
	a.Observe(ClassifyResult(err, statusCode), d)
}

// Merge folds another aggregator into a. The two must share a bucket cap.
// It is safe for concurrent use on both aggregators.
func (a *LatencyAggregator) Merge(other *LatencyAggregator) error {
	if a == nil || other == nil {
		return nil
	}
	other.mu.RLock()
	defer other.mu.RUnlock()
	if other.bucketCap != a.bucketCap {
		return errors.New("requester: cannot merge latency aggregators with different bucket caps")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	for i := range a.summaries {
		a.summaries[i].merge(other.summaries[i])
	}
	return nil
}

// Stats returns deterministic per-class statistics for the requested
// percentages (clamped to [1, 99]-style integer percentiles in [0, 100]).
// Percentages are de-duplicated and sorted; out-of-range values are dropped.
// The returned slice is always ordered by LatencyClass and is safe to retain.
func (a *LatencyAggregator) Stats(percentages []int) []LatencyClassStats {
	pcts := normalizePercentages(percentages)
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make([]LatencyClassStats, 0, latencyClassCount)
	for class := LatencyClass(0); int(class) < len(a.summaries); class++ {
		out = append(out, a.summaries[class].stats(class, pcts))
	}
	return out
}

// Snapshot returns a deep, independent copy of the aggregator.
func (a *LatencyAggregator) Snapshot() *LatencyAggregator {
	a.mu.RLock()
	defer a.mu.RUnlock()
	cp := &LatencyAggregator{bucketCap: a.bucketCap}
	for i := range a.summaries {
		cp.summaries[i] = a.summaries[i].clone()
	}
	return cp
}

func normalizePercentages(percentages []int) []int {
	if len(percentages) == 0 {
		percentages = DefaultPercentiles
	}
	valid := make([]int, 0, len(percentages))
	seen := make(map[int]struct{}, len(percentages))
	for _, p := range percentages {
		if p < 0 || p > 100 {
			continue
		}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		valid = append(valid, p)
	}
	sort.Ints(valid)
	return valid
}

// bucketIndex maps a latency in nanoseconds to a fixed histogram bin.
// Bins below the first octave keep unit-nanosecond resolution; each later
// octave is split into subBucketCnt log-spaced sub-buckets. Values outside
// the configured range saturate into the final bin.
func bucketIndex(ns int64, bucketCap int) int {
	if ns <= 0 {
		return 0
	}
	u := uint64(ns)
	exp := bits.Len64(u) - 1
	var idx int
	if exp < subBucketBits {
		idx = int(u)
	} else {
		mantissa := int((u >> uint(exp-subBucketBits)) & subBucketMask)
		idx = (exp-subBucketBits+1)<<subBucketBits | mantissa
	}
	if idx >= bucketCap {
		return bucketCap - 1
	}
	return idx
}

// bucketBounds returns the inclusive lower and exclusive upper nanosecond
// edges of a bin produced by bucketIndex.
func bucketBounds(idx int) (lo, hi int64) {
	if idx < subBucketCnt {
		return int64(idx), int64(idx + 1)
	}
	k := idx >> subBucketBits
	mantissa := idx & subBucketMask
	exp := k + subBucketBits - 1
	if exp >= 62 {
		// Defensive guard for oversized caller-supplied bucket caps.
		return int64(1) << 62, math.MaxInt64
	}
	shift := exp - subBucketBits
	lo = int64(1<<uint(exp) | mantissa<<uint(shift))
	hi = lo + 1<<uint(shift)
	return
}
