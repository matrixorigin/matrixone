package regexp2

import (
	"math"
	"math/big"
	"sync"
	"testing"
	"time"
)

func init() {
	//speed up testing by making the timeout clock 1ms instead of 100ms...
	//bad for benchmark tests though
	SetTimeoutCheckPeriod(time.Millisecond)
}
func TestDeadline(t *testing.T) {
	StopTimeoutClock()
	t.Cleanup(StopTimeoutClock)
	fast.current.write(41)
	if fasttime(42).reached() {
		t.Fatal("deadline expired before its tick")
	}
	fast.current.write(42)
	if !fasttime(42).reached() {
		t.Fatal("deadline did not expire at its tick")
	}
	fast.current.write(durationToTicks(time.Since(fast.start)))

	deadline := makeDeadline(clockPeriod)
	guard := time.NewTimer(time.Second)
	defer guard.Stop()
	poll := time.NewTicker(clockPeriod)
	defer poll.Stop()
	for !deadline.reached() {
		select {
		case <-poll.C:
		case <-guard.C:
			t.Fatal("updater failed to publish expiration")
		}
	}
}

func TestStopTimeoutClock(t *testing.T) {
	// run a quick regex with a long timeout
	// make sure the stop clock returns quickly
	r := MustCompile(".", 0)
	r.MatchTimeout = time.Second * 10

	if matched, err := r.MatchString("a"); err != nil || !matched {
		t.Fatalf("match: %v, %v", matched, err)
	}
	StopTimeoutClock()
	if want, got := false, fast.running; want != got {
		t.Errorf("Expected isRunning to be %v, got %v", want, got)
	}
}

// The clock publisher is deliberately absent: this is the state of a live
// updater descheduled before it can publish another tick. No timing luck is
// needed to exercise the deadline constructor.
func TestDeadlineIgnoresStalledPublisher(t *testing.T) {
	StopTimeoutClock()
	fast.mu.Lock()
	originalEpoch := fast.start
	fast.start = time.Now().Add(-10 * time.Second)
	fast.current.write(0)
	fast.clockEnd.write(fasttime(math.MaxInt64))
	fast.running = true
	fast.mu.Unlock()
	t.Cleanup(func() {
		fast.mu.Lock()
		fast.running = false
		fast.start = originalEpoch
		fast.current.write(durationToTicks(time.Since(fast.start)))
		fast.clockEnd.write(0)
		fast.mu.Unlock()
	})
	for _, timeout := range []time.Duration{0, 1, 1<<20 - 1, 1 << 20, time.Second, math.MaxInt64} {
		before := time.Since(fast.start)
		deadline := makeDeadline(timeout)
		after := time.Since(fast.start)
		// Tick arithmetic also keeps the independent oracle from overflowing.
		minimum := new(big.Int).Add(big.NewInt(int64(before)), big.NewInt(int64(timeout)))
		represented := new(big.Int).Lsh(big.NewInt(int64(deadline)), 20)
		maximum := durationToTicks(after) + durationToTicks(timeout) + durationToTicks(clockPeriod) + 3
		if represented.Cmp(minimum) < 0 || deadline > maximum {
			t.Fatalf("timeout %v: deadline %d precedes %s ns or exceeds %d ticks", timeout, deadline, minimum, maximum)
		}
	}
}

func TestDeadlineRestartsAndExtendsClock(t *testing.T) {
	StopTimeoutClock()
	originalEpoch := fast.start
	fast.start = time.Now().Add(-10 * time.Second)
	fast.current.write(0)
	fast.clockEnd.write(0)
	t.Cleanup(func() {
		StopTimeoutClock()
		fast.start = originalEpoch
		fast.current.write(durationToTicks(time.Since(fast.start)))
		fast.clockEnd.write(0)
	})
	var wg sync.WaitGroup
	deadlines := make([]fasttime, 16)
	for i := range deadlines {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			before := durationToTicks(time.Since(fast.start))
			deadlines[i] = makeDeadline(time.Duration(i+1) * time.Second)
			if deadlines[i] < before+durationToTicks(time.Duration(i+1)*time.Second) {
				t.Errorf("deadline %d predates construction", deadlines[i])
			}
		}(i)
	}
	wg.Wait()
	fast.mu.Lock()
	defer fast.mu.Unlock()
	if !fast.running {
		t.Fatal("updater did not restart")
	}
	for _, deadline := range deadlines {
		if deadline > fast.clockEnd.read() {
			t.Fatalf("horizon lost deadline %d", deadline)
		}
	}
}
