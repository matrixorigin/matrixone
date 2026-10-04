// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package malloc

import (
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const mmapReclaimBatch = 256
const mmapReclaimMaxDelay = 30 * time.Second

var mmapReclaimer = newMmapReclaimer()

// mmapMemory leaves pool hits and libc allocations untouched. Only a new mmap
// pays one atomic load. During reclamation failure, stop creating more mappings
// rather than silently accumulating an unbounded queue of failed frees.
func mmapMemory(size int) ([]byte, error) {
	if mmapReclaimer.blocked.Load() {
		return nil, unix.ENOMEM
	}
	return unix.Mmap(-1, 0, size, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS)
}

func unmapMemory(data []byte) {
	if err := unix.Munmap(data); err != nil {
		mmapReclaimer.deferUnmap(data, err)
	}
}

type pendingMmap struct {
	data []byte
	next *pendingMmap
}

// The queue owns failed mappings exclusively; they never return to a reuse pool.
// Count/bytes include the batch currently in the syscall loop. A single timer
// owns retries; enqueue never launches a worker while one is running. Admission
// stops at the first failure, bounding retention by existing live/pool mappings
// plus allocations already admitted concurrently. Frees must remain nonblocking
// on queue capacity: dropping a failed mapping would permanently leak it.
type mmapReclaim struct {
	blocked                atomic.Bool
	mu                     sync.Mutex
	head, tail             *pendingMmap
	count, bytes, failures uint64
	active                 bool
	started, lastReport    time.Time
	delay                  time.Duration
	sampleAddress          uintptr
	sampleSize             int
	sampleStack            [16]uintptr
	sampleStackLen         int

	// Immutable dependencies allow deterministic error, clock and timer tests.
	unmap    func([]byte) error
	schedule func(time.Duration, func())
	now      func() time.Time
	report   func(mmapReclaimReport)
}

type mmapReclaimReport struct {
	Pending, Bytes, Failures uint64
	Address                  uintptr
	Size                     int
	Elapsed                  time.Duration
	Stack                    [16]uintptr
	StackLen                 int
	ObservedAt               time.Time
}

func newMmapReclaimer() *mmapReclaim {
	return &mmapReclaim{
		unmap:    unix.Munmap,
		schedule: func(delay time.Duration, f func()) { time.AfterFunc(delay, f) },
		now:      time.Now,
		report:   reportMmapReclaim,
	}
}

func (r *mmapReclaim) appendLocked(p *pendingMmap) {
	if r.tail == nil {
		r.head = p
	} else {
		r.tail.next = p
	}
	r.tail = p
}

func (r *mmapReclaim) deferUnmap(data []byte, err error) {
	// EINVAL is an ownership/argument violation, not recoverable VMA pressure.
	// Do not turn corruption into retries or make an invalid mapping reusable.
	if err != unix.ENOMEM {
		panic(err)
	}
	r.mu.Lock()
	r.blocked.Store(true)
	r.appendLocked(&pendingMmap{data: data})
	r.bytes += uint64(len(data))
	r.count++
	r.failures++
	if !r.active {
		r.active = true
		r.started = r.now()
		r.delay = time.Second
		r.sampleAddress = uintptr(unsafe.Pointer(unsafe.SliceData(data)))
		r.sampleSize = len(data)
		r.sampleStackLen = runtime.Callers(2, r.sampleStack[:])
		r.schedule(r.delay, r.retry)
	}
	r.mu.Unlock()
}

func (r *mmapReclaim) retry() {
	r.mu.Lock()
	// Snapshot before retry so a quick recovery still leaves failure evidence.
	now := r.now()
	var report *mmapReclaimReport
	if r.lastReport.IsZero() || now.Sub(r.lastReport) >= mmapReclaimMaxDelay {
		report = &mmapReclaimReport{
			Pending: r.count, Bytes: r.bytes, Failures: r.failures,
			Address: r.sampleAddress, Size: r.sampleSize, Elapsed: now.Sub(r.started),
			Stack: r.sampleStack, StackLen: r.sampleStackLen,
			ObservedAt: now,
		}
		r.lastReport = now // retained across episodes to prevent flapping storms
	}
	batch := r.head
	var last *pendingMmap
	for n := 0; n < mmapReclaimBatch && r.head != nil; n++ {
		last = r.head
		r.head = r.head.next
	}
	if last != nil {
		last.next = nil
	}
	if r.head == nil {
		r.tail = nil
	}
	r.mu.Unlock()

	// No diagnostics or syscalls under the queue/cache lock. Report before
	// unmapping, otherwise the map-count evidence could disappear on recovery.
	if report != nil {
		r.report(*report)
	}
	var failedHead, failedTail *pendingMmap
	var freed, freedBytes, failures uint64
	for p := batch; p != nil; {
		next := p.next
		p.next = nil
		if err := r.unmap(p.data); err != nil {
			if err != unix.ENOMEM {
				panic(err)
			}
			if failedTail == nil {
				failedHead = p
			} else {
				failedTail.next = p
			}
			failedTail = p
			failures++
		} else {
			freed++
			freedBytes += uint64(len(p.data))
			p.data = nil
		}
		p = next
	}
	r.mu.Lock()
	r.count -= freed
	r.bytes -= freedBytes
	r.failures += failures
	if failedHead != nil {
		if r.tail == nil {
			r.head = failedHead
		} else {
			r.tail.next = failedHead
		}
		r.tail = failedTail
	}
	if r.count == 0 {
		r.active = false
		r.blocked.Store(false)
	} else {
		if freed > 0 {
			r.delay = time.Second
		} else {
			r.delay = min(r.delay*2, mmapReclaimMaxDelay)
		}
		r.schedule(r.delay, r.retry)
	}
	r.mu.Unlock()
}

// A stalled log sink must not stall reclamation or keep allocation admission
// closed. Start one process-lifetime writer lazily on the first failure; retain
// at most one additional report while it is blocked. Metrics remain available.
var mmapReclaimLogWriter = reclaimLogWriter{
	entries: make(chan mmapReclaimLog, 1), write: writeMmapReclaimLog,
}

type reclaimLogWriter struct {
	once    sync.Once
	entries chan mmapReclaimLog
	write   func(mmapReclaimLog)
}

type mmapReclaimLog struct {
	report      mmapReclaimReport
	diagnostics string
}

func reportMmapReclaim(r mmapReclaimReport) {
	mmapReclaimLogWriter.submit(mmapReclaimLog{r, mmapReclaimDiagnostics()})
}

func (w *reclaimLogWriter) submit(entry mmapReclaimLog) {
	w.once.Do(func() {
		go func() {
			for entry := range w.entries {
				w.write(entry)
			}
		}()
	})
	select {
	case w.entries <- entry:
	default:
	}
}

func writeMmapReclaimLog(entry mmapReclaimLog) {
	r := entry.report
	var stack strings.Builder
	frames := runtime.CallersFrames(r.Stack[:r.StackLen])
	for {
		frame, more := frames.Next()
		stack.WriteString(frame.Function)
		stack.WriteByte('\n')
		if !more {
			break
		}
	}
	logutil.Warn("mmap reclamation deferred; new mappings paused",
		zap.Time("observed_at", r.ObservedAt),
		zap.String("errno", "ENOMEM"), zap.Uint64("pending_mappings", r.Pending),
		zap.Uint64("pending_bytes", r.Bytes), zap.Uint64("failures_total", r.Failures),
		zap.Uint64("sample_address", uint64(r.Address)), zap.Int("sample_bytes", r.Size),
		zap.Duration("elapsed", r.Elapsed), zap.String("failure_stack", stack.String()),
		zap.String("vm_diagnostics", entry.diagnostics))
}

func init() {
	// Scrape-time snapshots: no metric operations on successful alloc/free.
	for _, metric := range []struct {
		name, help string
		value      func() float64
	}{
		{"pending_bytes", "Bytes owned by deferred mmap reclamation", func() float64 { return float64(mmapReclaimer.bytes) }},
		{"pending_mappings", "Mappings owned by deferred mmap reclamation", func() float64 { return float64(mmapReclaimer.count) }},
		{"failures_total", "Failed munmap attempts including retries", func() float64 { return float64(mmapReclaimer.failures) }},
	} {
		value := metric.value
		snapshot := func() float64 {
			mmapReclaimer.mu.Lock()
			defer mmapReclaimer.mu.Unlock()
			return value()
		}
		if metric.name == "failures_total" {
			v2.GetPrometheusRegistry().MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{
				Namespace: "mo", Subsystem: "mmap_reclaim", Name: metric.name, Help: metric.help,
			}, snapshot))
		} else {
			v2.GetPrometheusRegistry().MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Namespace: "mo", Subsystem: "mmap_reclaim", Name: metric.name, Help: metric.help,
			}, snapshot))
		}
	}
}
