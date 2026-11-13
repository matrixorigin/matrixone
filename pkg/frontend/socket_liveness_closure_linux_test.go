// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type bufferedRequestReadAheadConn struct {
	net.Conn
	initial int
}

func (c *bufferedRequestReadAheadConn) Read(p []byte) (int, error) {
	if c.initial == 0 {
		return c.Conn.Read(p)
	}
	want := c.initial
	c.initial = 0
	if len(p) < want {
		return 0, io.ErrShortBuffer
	}
	return io.ReadFull(c.Conn, p[:want])
}

func TestLivenessMonitorProcessesBufferedRequestsBeforeEOF(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	require.NoError(t, server.SetDeadline(time.Now().Add(10*time.Second)))
	require.NoError(t, client.SetDeadline(time.Now().Add(10*time.Second)))
	sv, err := getSystemVariables("test/system_vars_config.toml")
	require.NoError(t, err)
	first := makePacket([]byte("first"), 0)
	second := makePacket([]byte("second"), 1)
	both := append(append([]byte(nil), first...), second...)
	allocator := NewLeakCheckAllocator()
	conn, err := NewIOSessionWithOptions(&bufferedRequestReadAheadConn{Conn: server, initial: len(both)}, config.NewParameterUnit(sv, nil, nil, nil), "", WithIOSessionAllocator(allocator))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(); require.True(t, allocator.CheckBalance()) })
	n, err := client.Write(both)
	require.NoError(t, err)
	require.Equal(t, len(both), n)
	require.NoError(t, client.(*net.TCPConn).CloseWrite())
	require.Eventually(t, func() bool { return terminalSocketHasEvent(server.(*net.TCPConn), unix.POLLRDHUP) }, 3*time.Second, time.Millisecond)
	payload, err := conn.Read()
	require.NoError(t, err)
	require.Equal(t, "first", string(payload))
	require.Equal(t, len(second), conn.fixBuf.AvailableDataLen(), "the second complete request must already be buffered")
	conn.UseConn(server)
	routine := NewRoutine(context.Background(), &testMysqlWriter{}, &config.FrontendParameters{})
	t.Cleanup(routine.cancelRoutineFunc)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	routine.setCancelRequestFunc(cancel)
	require.True(t, routine.setInProcessRequest(true))
	monitor := connectionLivenessMonitor{rm: &RoutineManager{clients: map[*Conn]*Routine{conn: routine}}}
	monitor.scan(time.Now())
	require.NoError(t, ctx.Err(), "EOF cannot cancel the active buffered request")
	payload, err = conn.Read()
	require.NoError(t, err)
	require.Equal(t, "second", string(payload))
	_, err = conn.Read()
	require.ErrorIs(t, err, io.EOF)
}

func TestLivenessProbeConcurrentLocalClose(t *testing.T) {
	server, client := tcpConnectionPair(t)
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	probe := newTerminalSocketProbe(server)
	require.NotNil(t, probe)
	original := probe.control
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseProbe := func() { once.Do(func() { close(release) }) }
	probe.control = func(fd uintptr) { close(entered); <-release; original(fd) }
	probeDone := make(chan struct{})
	result := make(chan bool, 1)
	var closeDone chan struct{}
	t.Cleanup(func() {
		releaseProbe()
		_ = server.Close()
		select {
		case <-probeDone:
		case <-time.After(5 * time.Second):
			t.Error("probe did not stop")
		}
		if closeDone != nil {
			select {
			case <-closeDone:
			case <-time.After(5 * time.Second):
				t.Error("close did not stop")
			}
		}
	})
	go func() { defer close(probeDone); result <- probe.terminal() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("probe did not reach RawConn.Control")
	}
	closeDone = make(chan struct{})
	closeStarted := make(chan struct{})
	closeErr := make(chan error, 1)
	go func() { defer close(closeDone); close(closeStarted); closeErr <- server.Close() }()
	<-closeStarted
	// The first Control callback still pins the fd. Wait until an independent
	// Control sees net.ErrClosed, proving Close has marked it closing before
	// allowing the original probe to finish.
	require.Eventually(t, func() bool {
		err := probe.raw.Control(func(uintptr) {})
		return errors.Is(err, net.ErrClosed)
	}, 5*time.Second, time.Millisecond, "Close must enter the closing state while the probe holds the fd")
	select {
	case <-closeDone:
		t.Fatal("local Close returned while the original Control callback was still active")
	default:
	}
	releaseProbe()
	select {
	case closed := <-result:
		require.False(t, closed, "local close cannot be attributed to a peer reset")
	case <-time.After(5 * time.Second):
		t.Fatal("probe blocked")
	}
	select {
	case <-closeDone:
		require.NoError(t, <-closeErr)
	case <-time.After(5 * time.Second):
		t.Fatal("local close blocked")
	}
	probe.control = original
	require.False(t, probe.terminal())
}
