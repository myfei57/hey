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

// Package requester provides commands to run load tests and display results.
package requester

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

// Max size of the buffer of result channel.
const maxResult = 1000000
const maxIdleConn = 500

// ErrParamsVersionMismatch is returned by Resume when the supplied parameter
// version does not match the version of the running Work. A mismatch means
// the snapshot was produced by a different scheduling configuration, so
// resuming from it is refused and the scheduler stays paused.
var ErrParamsVersionMismatch = errors.New("requester: parameter version mismatch, cannot resume snapshot")

type result struct {
	err           error
	statusCode    int
	offset        time.Duration
	duration      time.Duration
	connDuration  time.Duration // connection setup(DNS lookup + Dial up) duration
	dnsDuration   time.Duration // dns lookup duration
	reqDuration   time.Duration // request "write" duration
	resDuration   time.Duration // response "read" duration
	delayDuration time.Duration // delay between response and request
	contentLength int64
}

type Work struct {
	// Request is the request to be made.
	Request *http.Request

	RequestBody []byte

	// RequestFunc is a function to generate requests. If it is nil, then
	// Request and RequestData are cloned for each request.
	RequestFunc func() *http.Request

	// N is the total number of requests to make.
	N int

	// C is the concurrency level, the number of concurrent workers to run.
	C int

	// H2 is an option to make HTTP/2 requests
	H2 bool

	// Timeout in seconds.
	Timeout int

	// Qps is the rate limit in queries per second.
	QPS float64

	// DisableCompression is an option to disable compression in response
	DisableCompression bool

	// DisableKeepAlives is an option to prevents re-use of TCP connections between different HTTP requests
	DisableKeepAlives bool

	// DisableRedirects is an option to prevent the following of HTTP redirects
	DisableRedirects bool

	// Output represents the output type. If "csv" is provided, the
	// output will be dumped as a csv stream.
	Output string

	// ProxyAddr is the address of HTTP proxy server in the format on "host:port".
	// Optional.
	ProxyAddr *url.URL

	// Writer is where results will be written. If nil, results are written to stdout.
	Writer io.Writer

	// ParamsVersion identifies the scheduling parameters of this Work. A
	// scheduler snapshot carries the same version and Resume refuses to
	// resume a snapshot whose version differs. When left as zero, Init
	// derives a stable version from the Work parameters automatically.
	ParamsVersion uint64

	initOnce sync.Once
	results  chan *result
	stopCh   chan struct{}
	start    time.Duration

	report *report

	// Pause/resume gate. pauseMu only guards swapping of the gate channel;
	// it is never held while a request is in flight or while a result is
	// produced, so pausing cannot block the statistics path. resumeCh is
	// non-nil (and open) while paused and gets closed on resume.
	pauseMu  sync.Mutex
	resumeCh chan struct{}
	paused   atomic.Bool

	// Lock-free scheduler counters, read exclusively via atomic loads by
	// Snapshot. Capturing a snapshot therefore never takes a lock and
	// never blocks the workers or the reporter goroutine. The in-flight
	// count is derived as sent-succeeded-failed so the accounting
	// identity Sent == InFlight + Succeeded + Failed holds for every
	// snapshot without needing a transactional read.
	sent       atomic.Int64 // requests already dispatched to workers
	succeeded  atomic.Int64 // completed requests without error
	failed     atomic.Int64 // completed requests with an error
	nextTicket atomic.Int64 // central claim counter; each ticket is handed out at most once
}

func (b *Work) writer() io.Writer {
	if b.Writer == nil {
		return os.Stdout
	}
	return b.Writer
}

// Init initializes internal data-structures
func (b *Work) Init() {
	b.initOnce.Do(func() {
		b.results = make(chan *result, min(b.C*1000, maxResult))
		b.stopCh = make(chan struct{}, b.C)
		if b.ParamsVersion == 0 {
			b.ParamsVersion = b.computeParamsVersion()
		}
	})
}

// SchedulerSnapshot is a point-in-time view of the request scheduler while
// workers are running. The counters are sampled with atomic loads, so
// capturing a snapshot never blocks the workers or the statistics path.
type SchedulerSnapshot struct {
	// Total is the planned number of requests (Work.N).
	Total int64
	// Sent is the number of requests already dispatched to workers,
	// including the ones in progress and the completed ones.
	// Sent == InFlight + Succeeded + Failed.
	Sent int64
	// InFlight is the number of requests currently in progress, derived as
	// Sent - Succeeded - Failed.
	InFlight int64
	// Succeeded is the number of completed requests without error.
	Succeeded int64
	// Failed is the number of completed requests with an error.
	Failed int64
	// Remaining is the number of requests that have not been dispatched
	// yet. Remaining == Total - Sent.
	Remaining int64
	// Paused reports whether the scheduler was paused at capture time.
	Paused bool
	// ParamsVersion is the parameter version of the owning Work. It has to
	// be presented unchanged when resuming from this snapshot.
	ParamsVersion uint64
}

// Snapshot captures the current scheduler counters. It is lock-free: every
// counter is read with an atomic load and neither the result channel nor any
// worker/reporter lock is touched, so statistics collection is never blocked.
// InFlight is derived from the other counters, hence the identity
// Sent == InFlight + Succeeded + Failed holds for every captured snapshot.
func (b *Work) Snapshot() SchedulerSnapshot {
	sent := b.sent.Load()
	succeeded := b.succeeded.Load()
	failed := b.failed.Load()
	return SchedulerSnapshot{
		Total:         int64(b.N),
		Sent:          sent,
		InFlight:      sent - succeeded - failed,
		Succeeded:     succeeded,
		Failed:        failed,
		Remaining:     int64(b.N) - sent,
		Paused:        b.paused.Load(),
		ParamsVersion: b.ParamsVersion,
	}
}

// Pause stops workers from dispatching new requests. Requests already in
// flight are allowed to finish and their results keep flowing to the
// reporter, so the statistics path is not blocked. Pause is idempotent and
// returns immediately without waiting for in-flight requests to complete.
func (b *Work) Pause() {
	b.pauseMu.Lock()
	defer b.pauseMu.Unlock()
	if b.paused.Load() {
		return
	}
	// Install the gate before flagging the pause so workers never observe
	// a paused state without a channel to wait on.
	b.resumeCh = make(chan struct{})
	b.paused.Store(true)
}

// Resume unblocks paused workers and returns a fresh snapshot. The
// paramsVersion must match the Work's parameter version (typically the
// ParamsVersion carried by a captured SchedulerSnapshot); on mismatch
// ErrParamsVersionMismatch is returned and the scheduler remains paused.
// Resuming a Work that is not paused is a no-op and returns the snapshot.
func (b *Work) Resume(paramsVersion uint64) (SchedulerSnapshot, error) {
	if paramsVersion != b.ParamsVersion {
		return b.Snapshot(), ErrParamsVersionMismatch
	}
	b.pauseMu.Lock()
	if b.paused.Load() {
		close(b.resumeCh)
		b.resumeCh = nil
		b.paused.Store(false)
	}
	b.pauseMu.Unlock()
	return b.Snapshot(), nil
}

// waitWhilePaused blocks while the scheduler is paused. It returns true when
// the worker may continue and false when the worker must stop.
func (b *Work) waitWhilePaused() bool {
	for {
		b.pauseMu.Lock()
		gate := b.resumeCh
		b.pauseMu.Unlock()
		if gate == nil {
			return true
		}
		select {
		case <-b.stopCh:
			return false
		case <-gate:
			// Resumed; loop again to observe a possible re-pause.
		}
	}
}

// computeParamsVersion derives a stable version from the scheduling-relevant
// parameters of the Work. Two Works with identical parameters produce the
// same version, while any parameter change invalidates an old snapshot.
func (b *Work) computeParamsVersion() uint64 {
	h := fnv.New64a()
	fmt.Fprintf(h, "n=%d|c=%d|qps=%g|timeout=%d|h2=%t", b.N, b.C, b.QPS, b.Timeout, b.H2)
	fmt.Fprintf(h, "|compression=%t|keepalives=%t|redirects=%t|output=%s",
		b.DisableCompression, b.DisableKeepAlives, b.DisableRedirects, b.Output)
	fmt.Fprintf(h, "|requestFunc=%t", b.RequestFunc != nil)
	if b.Request != nil {
		fmt.Fprintf(h, "|method=%s|url=%s", b.Request.Method, b.Request.URL.String())
	}
	if b.ProxyAddr != nil {
		fmt.Fprintf(h, "|proxy=%s", b.ProxyAddr.String())
	}
	return h.Sum64()
}

// Run makes all the requests, prints the summary. It blocks until
// all work is done.
func (b *Work) Run() {
	b.Init()
	b.start = now()
	b.report = newReport(b.writer(), b.results, b.Output, b.N)
	// Run the reporter first, it polls the result channel until it is closed.
	go func() {
		runReporter(b.report)
	}()
	b.runWorkers()
	b.Finish()
}

func (b *Work) Stop() {
	// Send stop signal so that workers can stop gracefully.
	for i := 0; i < b.C; i++ {
		b.stopCh <- struct{}{}
	}
}

func (b *Work) Finish() {
	close(b.results)
	total := now() - b.start
	// Wait until the reporter is done.
	<-b.report.done
	b.report.finalize(total)
}

func (b *Work) makeRequest(c *http.Client) error {
	s := now()
	var size int64
	var code int
	var dnsStart, connStart, resStart, reqStart, delayStart time.Duration
	var dnsDuration, connDuration, resDuration, reqDuration, delayDuration time.Duration
	var req *http.Request
	if b.RequestFunc != nil {
		req = b.RequestFunc()
	} else {
		req = cloneRequest(b.Request, b.RequestBody)
	}
	trace := &httptrace.ClientTrace{
		DNSStart: func(info httptrace.DNSStartInfo) {
			dnsStart = now()
		},
		DNSDone: func(dnsInfo httptrace.DNSDoneInfo) {
			dnsDuration = now() - dnsStart
		},
		GetConn: func(h string) {
			connStart = now()
		},
		GotConn: func(connInfo httptrace.GotConnInfo) {
			if !connInfo.Reused {
				connDuration = now() - connStart
			}
			reqStart = now()
		},
		WroteRequest: func(w httptrace.WroteRequestInfo) {
			reqDuration = now() - reqStart
			delayStart = now()
		},
		GotFirstResponseByte: func() {
			delayDuration = now() - delayStart
			resStart = now()
		},
	}
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
	resp, err := c.Do(req)
	if err == nil {
		size = resp.ContentLength
		code = resp.StatusCode
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	t := now()
	resDuration = t - resStart
	finish := t - s
	b.results <- &result{
		offset:        s,
		statusCode:    code,
		duration:      finish,
		err:           err,
		contentLength: size,
		connDuration:  connDuration,
		dnsDuration:   dnsDuration,
		reqDuration:   reqDuration,
		resDuration:   resDuration,
		delayDuration: delayDuration,
	}
	return err
}

func (b *Work) runWorker(client *http.Client) {
	var throttle <-chan time.Time
	if b.QPS > 0 {
		ticker := time.NewTicker(time.Duration(1e6/(b.QPS)) * time.Microsecond)
		defer ticker.Stop()
		throttle = ticker.C
	}

	if b.DisableRedirects {
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}
	for {
		// Check if application is stopped. Do not send into a closed channel.
		select {
		case <-b.stopCh:
			return
		default:
			// Block here while paused. Requests already in flight are not
			// affected and their results still reach the reporter.
			if !b.waitWhilePaused() {
				return
			}
			if b.QPS > 0 {
				<-throttle
			}
			// Claim the next request ticket atomically. A ticket is handed
			// out at most once for the whole Work, including across
			// pause/resume cycles, so every request is sent at most once.
			ticket := b.nextTicket.Add(1) - 1
			if ticket >= int64(b.N) {
				return
			}
			b.sent.Add(1)
			err := b.makeRequest(client)
			if err != nil {
				b.failed.Add(1)
			} else {
				b.succeeded.Add(1)
			}
		}
	}
}

func (b *Work) runWorkers() {
	var wg sync.WaitGroup
	wg.Add(b.C)

	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         b.Request.Host,
		},
		MaxIdleConnsPerHost: min(b.C, maxIdleConn),
		DisableCompression:  b.DisableCompression,
		DisableKeepAlives:   b.DisableKeepAlives,
		Proxy:               http.ProxyURL(b.ProxyAddr),
	}
	if b.H2 {
		http2.ConfigureTransport(tr)
	} else {
		tr.TLSNextProto = make(map[string]func(string, *tls.Conn) http.RoundTripper)
	}
	client := &http.Client{Transport: tr, Timeout: time.Duration(b.Timeout) * time.Second}

	// Workers draw request tickets from the shared atomic counter instead
	// of each owning a fixed slice of requests, which keeps accounting
	// exact (all N requests are dispatched) even across pause/resume.
	for i := 0; i < b.C; i++ {
		go func() {
			b.runWorker(client)
			wg.Done()
		}()
	}
	wg.Wait()
}

// cloneRequest returns a clone of the provided *http.Request.
func cloneRequest(r *http.Request, body []byte) *http.Request {
	r2 := r.Clone(r.Context())
	if len(body) > 0 {
		r2.Body = io.NopCloser(bytes.NewReader(body))
	}
	return r2
}
