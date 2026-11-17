// Copyright 2026 Matrix Origin
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

//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/mdlayher/socket"
	"golang.org/x/sys/unix"
)

// Conntrack's netlink receiver uses socket.Recvmsg with a background context.
// Exercise that dependency without CAP_NET_ADMIN or changes to host conntrack.
func conntrackSocketPair(t *testing.T) (*socket.Conn, *socket.Conn) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	files := [2]*os.File{os.NewFile(uintptr(fds[0]), "receiver"), os.NewFile(uintptr(fds[1]), "sender")}
	for _, file := range files {
		defer file.Close()
	}
	var conns [2]*socket.Conn
	for i, file := range files {
		// FileConn duplicates the descriptor; the original is closed above.
		conns[i], err = socket.FileConn(file, file.Name())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conns[i].Close() })
	}
	return conns[0], conns[1]
}

func TestConntrackSocketBackgroundReceive(t *testing.T) {
	receiver, sender := conntrackSocketPair(t)
	// These are hang guards, not scheduling or performance thresholds.
	if err := receiver.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := sender.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for i := range 32 {
		want := byte(i)
		if _, err := sender.Sendmsg(context.Background(), []byte{want}, nil, nil, 0); err != nil {
			t.Fatal(err)
		}
		var buf [1]byte
		n, _, _, _, err := receiver.Recvmsg(context.Background(), buf[:], nil, 0)
		if err != nil || n != 1 || buf[0] != want {
			t.Fatalf("receive %d: n=%d data=%v err=%v", i, n, buf, err)
		}
	}
}

// Cancel after socket's initial Err check but while it reads the deadline.
// This deterministically tests cancellation of an admitted operation with a
// distant deadline, instead of relying on a sleep to get the reader blocked.
type cancelOnDeadlineContext struct {
	context.Context
	cancel context.CancelFunc
}

func (c cancelOnDeadlineContext) Deadline() (time.Time, bool) {
	deadline, ok := c.Context.Deadline()
	c.cancel()
	return deadline, ok
}

func TestConntrackSocketCancelAndReuse(t *testing.T) {
	receiver, sender := conntrackSocketPair(t)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	t.Cleanup(cancel)
	result := make(chan error, 1)
	go func() {
		var buf [1]byte
		_, _, _, _, err := receiver.Recvmsg(cancelOnDeadlineContext{ctx, cancel}, buf[:], nil, 0)
		result <- err
	}()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-timer.C:
		// Release and join the test-owned reader even on the old dependency.
		_ = receiver.Close()
		<-result
		t.Fatal("Recvmsg ignored cancellation while a distant deadline was set")
	}
	// A cancelled watcher must finish before resetting the socket deadline;
	// otherwise it can poison the next receive with a deadline in the past.
	if _, err := sender.Sendmsg(context.Background(), []byte{42}, nil, nil, 0); err != nil {
		t.Fatal(err)
	}
	var buf [1]byte
	n, _, _, _, err := receiver.Recvmsg(context.Background(), buf[:], nil, 0)
	if err != nil || n != 1 || buf[0] != 42 {
		t.Fatalf("receive after cancellation: n=%d data=%v err=%v", n, buf, err)
	}
}

type admittedReceiveContext struct {
	context.Context
	admitted chan<- struct{}
	release  <-chan struct{}
}

func (c admittedReceiveContext) Err() error {
	c.admitted <- struct{}{}
	<-c.release
	return nil
}

func TestConntrackSocketCloseReceivers(t *testing.T) {
	receiver, _ := conntrackSocketPair(t)
	const workers = 16
	var wg sync.WaitGroup
	results := make(chan error, workers)
	admitted := make(chan struct{}, workers)
	release := make(chan struct{})
	ctx := admittedReceiveContext{context.Background(), admitted, release}
	for range workers {
		wg.Go(func() {
			var buf [1]byte
			_, _, _, _, err := receiver.Recvmsg(ctx, buf[:], nil, 0)
			results <- err
		})
	}
	// Every reader has passed the socket's initial closed check. Close must
	// still terminate them, even though they have not reached the poller yet.
	for range workers {
		<-admitted
	}
	if err := receiver.Close(); err != nil {
		t.Error(err)
	}
	close(release)
	wg.Wait()
	close(results)
	for err := range results {
		// RawConn returns internal/poll.ErrFileClosing, not os.ErrClosed.
		// Conntrack's eventWorker relies on this same error string.
		if err == nil || err.Error() != "use of closed file" {
			t.Errorf("receive after Close: %v", err)
		}
	}
	var buf [1]byte
	_, _, _, _, err := receiver.Recvmsg(context.Background(), buf[:], nil, 0)
	if !errors.Is(err, unix.EBADF) {
		t.Fatalf("new receive on closed socket: %v", err)
	}
}
