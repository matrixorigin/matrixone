// Copyright 2021 - 2022 Matrix Origin
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

package tests

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/testutil/portlease"
	"github.com/stretchr/testify/require"
)

func closePortAllocator(t *testing.T, a *portAllocator) {
	t.Helper()
	t.Cleanup(func() {
		for _, lease := range a.leases {
			require.NoError(t, lease.Close())
		}
	})
}

func TestGetAvailablePort(t *testing.T) {
	a := &portAllocator{}
	closePortAllocator(t, a)
	first, err := a.allocate("127.0.0.1")
	require.NoError(t, err)
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", first))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, listener.Close()) })
	// Revisit an occupied candidate: it must be skipped, never published.
	a.next, err = strconv.ParseUint(first, 10, 64)
	require.NoError(t, err)
	second, err := a.allocate("127.0.0.1")
	require.NoError(t, err)
	require.NotEqual(t, first, second)
}

func TestGetAddressBatch(t *testing.T) {
	addrs, err := GetAddressBatch("127.0.0.1", 3)
	require.NoError(t, err)
	require.Len(t, addrs, 3)
	require.Len(t, map[string]bool{addrs[0]: true, addrs[1]: true, addrs[2]: true}, 3)
	for _, addr := range addrs {
		host, port, err := net.SplitHostPort(addr)
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1", host)
		_, err = strconv.Atoi(port)
		require.NoError(t, err)
	}
}

func TestPortAllocatorRangeRollover(t *testing.T) {
	a := &portAllocator{}
	closePortAllocator(t, a)
	ports := make(map[string]bool)
	for i := uint64(0); i <= portlease.Span; i++ {
		port, err := a.allocate("127.0.0.1")
		require.NoError(t, err)
		require.False(t, ports[port])
		ports[port] = true
	}
	require.GreaterOrEqual(t, len(a.leases), 2)
	// Rollover must retain ownership of earlier, potentially still-unbound ports.
	for _, lease := range a.leases {
		contender, locked, err := a.ranges.TryAcquire(lease.Base())
		require.NoError(t, err)
		if contender != nil {
			require.NoError(t, contender.Close())
		}
		require.False(t, locked)
	}
}

func TestPortAllocatorConcurrent(t *testing.T) {
	a := &portAllocator{}
	closePortAllocator(t, a)
	const count = 32
	results := make(chan string, count)
	failures := make(chan error, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			port, err := a.allocate("127.0.0.1")
			if err != nil {
				failures <- err
			} else {
				results <- port
			}
		}()
	}
	wg.Wait()
	close(results)
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	ports := make(map[string]bool)
	for port := range results {
		require.False(t, ports[port])
		ports[port] = true
	}
	require.Len(t, ports, count)
}

func TestPortAllocatorExhaustion(t *testing.T) {
	a := &portAllocator{ranges: portlease.Allocator{Directory: t.TempDir()}}
	closePortAllocator(t, a)
	// Lock-only exhaustion requires no TCP listeners or thousands of probes.
	for i := uint64(0); i < (portlease.MaxPort-portlease.MinPort)/portlease.Span; i++ {
		lease, err := a.ranges.Acquire()
		require.NoError(t, err)
		a.leases = append(a.leases, lease)
	}
	_, err := a.allocate("127.0.0.1")
	require.ErrorContains(t, err, "no test port range")
	require.NoError(t, a.leases[len(a.leases)-1].Close())
	a.leases = a.leases[:len(a.leases)-1]
	// An exhausted scan must not permanently poison subsequent acquisition.
	lease, err := a.ranges.Acquire()
	require.NoError(t, err)
	a.leases = append(a.leases, lease)
}

func TestGetAvailablePortAcrossProcesses(t *testing.T) {
	if os.Getenv("MO_PORT_ALLOCATION_HELPER") == "1" {
		runPortAllocationHelper(t)
		return
	}
	type child struct {
		cmd    *exec.Cmd
		input  io.WriteCloser
		output *bufio.Reader
		cancel context.CancelFunc
		waited bool
	}
	children := make([]*child, 0, 2)
	t.Cleanup(func() {
		for _, c := range children {
			c.cancel()
			if !c.waited {
				_ = c.cmd.Wait()
			}
		}
	})
	ports := make(map[string]bool)
	for i := 0; i < 2; i++ {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
		c := &child{cancel: cancel}
		args := []string{"-test.run=^TestGetAvailablePortAcrossProcesses$", "-test.timeout=20s"}
		if dir := flag.Lookup("test.gocoverdir"); dir != nil && dir.Value.String() != "" {
			args = append(args, "-test.gocoverdir="+dir.Value.String())
		}
		c.cmd = exec.CommandContext(ctx, os.Args[0], args...)
		c.cmd.Env = append(os.Environ(), "MO_PORT_ALLOCATION_HELPER=1", "TMPDIR="+t.TempDir(), fmt.Sprintf("MO_PORT_BIND_HOST=%s", []string{"127.0.0.1", "0.0.0.0"}[i]))
		c.cmd.Stderr = os.Stderr
		var err error
		c.input, err = c.cmd.StdinPipe()
		require.NoError(t, err)
		out, err := c.cmd.StdoutPipe()
		require.NoError(t, err)
		c.output = bufio.NewReader(out)
		require.NoError(t, c.cmd.Start())
		children = append(children, c)
		line, err := c.output.ReadString('\n')
		require.NoError(t, err)
		require.True(t, strings.HasPrefix(line, "PORTS "), line)
		for _, port := range strings.Fields(strings.TrimPrefix(line, "PORTS ")) {
			require.False(t, ports[port], "duplicate unpublished port %s", port)
			ports[port] = true
		}
	}
	require.Len(t, ports, 4)
	// Both allocation reports arrive before either process may bind.
	for _, c := range children {
		_, err := io.WriteString(c.input, "b")
		require.NoError(t, err)
		line, err := c.output.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "BOUND\n", line)
	}
	// Both children hold their listeners until the parent acknowledges them.
	for _, c := range children {
		require.NoError(t, c.input.Close())
		err := c.cmd.Wait()
		c.waited = true
		require.NoError(t, err)
	}
}

func runPortAllocationHelper(t *testing.T) {
	port, err := GetAvailablePort("127.0.0.1")
	require.NoError(t, err)
	addrs, err := GetAddressBatch("127.0.0.1", 1)
	require.NoError(t, err)
	_, second, err := net.SplitHostPort(addrs[0])
	require.NoError(t, err)
	fmt.Printf("PORTS %s %s\n", port, second)
	var signal [1]byte
	_, err = io.ReadFull(os.Stdin, signal[:])
	require.NoError(t, err)
	listeners := make([]net.Listener, 0, 2)
	defer func() {
		for _, ln := range listeners {
			require.NoError(t, ln.Close())
		}
	}()
	for _, p := range []string{port, second} {
		ln, err := net.Listen("tcp", net.JoinHostPort(os.Getenv("MO_PORT_BIND_HOST"), p))
		require.NoError(t, err)
		listeners = append(listeners, ln)
		// Verify an actual connection, including wildcard-to-loopback reachability.
		conn, err := net.DialTimeout("tcp", net.JoinHostPort("127.0.0.1", p), time.Second)
		require.NoError(t, err)
		accepted, err := ln.Accept()
		require.NoError(t, err)
		require.NoError(t, conn.Close())
		require.NoError(t, accepted.Close())
	}
	fmt.Println("BOUND")
	_, err = io.ReadFull(os.Stdin, signal[:])
	require.ErrorIs(t, err, io.EOF)
}
