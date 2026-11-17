// Copyright 2021 Matrix Origin
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
	"fmt"
	"net"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/ti-mo/conntrack"
	"github.com/ti-mo/netfilter"
	"go.uber.org/zap"
)

const (
	connsThreshold         = 1024
	conntrackRetryInterval = 5 * time.Second
)

var (
	conntrackReportInterval = time.Second * 5
	errConntrackClose       = errors.New("conntrack socket close failed")
)

type conntrackClient interface {
	Close() error
	Listen(chan<- conntrack.Event, uint8, []netfilter.NetlinkGroup) (chan error, error)
}

var dialConntrack = func() (conntrackClient, error) {
	return conntrack.Dial(nil)
}

func startConnectionTracking() (err error) {
	defer func() {
		if err != nil {
			logutil.Error("conntrack: error", zap.Error(err))
		}
	}()

	// Capture the factory once; every replacement has the same owner and policy.
	dial := dialConntrack
	listener, err := listenConntrack(dial)
	if err != nil {
		return err
	}
	// Keep the existing process lifetime. Runtime replacement only happens after
	// the worker reports a fatal error; it is then safe to join that worker.
	go runConnectionTracking(context.Background(), listener, dial)
	return nil
}

// A listener owns one socket and its queues. Replacements never share queues
// or the consumer's map, so an old generation cannot update the new one.
type conntrackListener struct {
	client conntrackClient
	events chan conntrack.Event
	errors chan error
}

func listenConntrack(dial func() (conntrackClient, error)) (*conntrackListener, error) {
	c, err := dial()
	if err != nil {
		return nil, err
	}
	events := make(chan conntrack.Event, 65536)
	// Keep receive, decode and delivery ordered. Concurrent conntrack workers
	// can deliver DESTROY before NEW, leaving a destroyed flow in activeConns.
	errorChan, err := c.Listen(events, 1, []netfilter.NetlinkGroup{
		netfilter.GroupCTNew,
		netfilter.GroupCTDestroy,
	})
	if err != nil {
		if closeErr := c.Close(); closeErr != nil {
			return nil, errors.Join(err, errConntrackClose, closeErr)
		}
		return nil, err
	}
	return &conntrackListener{client: c, events: events, errors: errorChan}, nil
}

func (l *conntrackListener) close() error {
	// conntrack.Close closes the socket, then waits for its worker. The worker
	// may be blocked publishing an event/error, which closing the fd cannot
	// interrupt. Keep draining both queues until Close has joined the worker.
	done := make(chan error, 1)
	go func() {
		done <- l.client.Close()
	}()
	for {
		select {
		case err := <-done:
			return err
		case _, ok := <-l.events:
			if !ok {
				l.events = nil
			}
		case _, ok := <-l.errors:
			if !ok {
				l.errors = nil
			}
		}
	}
}

func runConnectionTracking(ctx context.Context, listener *conntrackListener, dial func() (conntrackClient, error)) {
	for {
		err := logConnTrack(ctx, listener.events, listener.errors)
		closeErr := listener.close()
		if closeErr != nil {
			// Do not accumulate replacements when the old socket cannot be closed.
			logutil.Error("conntrack: tracking stopped", zap.Error(closeErr))
			return
		}
		if ctx.Err() != nil {
			return
		}
		logutil.Warn("conntrack: tracking interrupted; previous statistics invalid", zap.Error(err))
		for {
			// This is optional diagnostics, not a service admission gate. Retry at
			// most once per interval, including repeated Dial/Listen failures.
			timer := time.NewTimer(conntrackRetryInterval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			if ctx.Err() != nil {
				return
			}
			listener, err = listenConntrack(dial)
			if err == nil {
				break
			}
			logutil.Error("conntrack: restart failed", zap.Error(err))
			if errors.Is(err, errConntrackClose) {
				return
			}
		}
	}
}

func logConnTrack(ctx context.Context, events chan conntrack.Event, errorChan chan error) error {

	activeConns := make(map[uint32]*conntrack.Flow)
	ticker := time.NewTicker(conntrackReportInterval)
	defer ticker.Stop()
	startedAt := time.Now()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		select {

		case <-ctx.Done():
			return ctx.Err()

		case ev, ok := <-events:
			if !ok {
				return errors.New("conntrack event channel closed")
			}
			switch ev.Type {

			case conntrack.EventNew:
				activeConns[ev.Flow.ID] = ev.Flow

			case conntrack.EventDestroy:
				delete(activeConns, ev.Flow.ID)

			}

		case err, ok := <-errorChan:
			if !ok || err == nil {
				return errors.New("conntrack listener stopped without an error")
			}
			// Listen has one worker, and every reported error terminates it.
			// Do not keep publishing stale counts after that worker exits.
			return err

		case <-ticker.C:
			logutil.Info("conntrack: stats",
				zap.Any("connections", len(activeConns)),
				zap.String("scope", "since_listener_start"),
				zap.Bool("complete", false),
				zap.Time("listener_started_at", startedAt),
			)

			logConnectionStats(activeConns)

		}
	}
}

func logConnectionStats(actives map[uint32]*conntrack.Flow) {
	srcHostPorts := make(map[string]int)
	destHostPorts := make(map[string]int)
	for _, flow := range actives {
		src, dest := flowGetHostPorts(flow)
		srcHostPorts[src]++
		destHostPorts[dest]++
	}
	logHostPortStats(srcHostPorts, "source")
	logHostPortStats(destHostPorts, "destination")
}

func flowGetHostPorts(flow *conntrack.Flow) (string, string) {
	return net.JoinHostPort(
			flow.TupleOrig.IP.SourceAddress.String(),
			fmt.Sprintf("%d", flow.TupleOrig.Proto.SourcePort),
		), net.JoinHostPort(
			flow.TupleOrig.IP.DestinationAddress.String(),
			fmt.Sprintf("%d", flow.TupleOrig.Proto.DestinationPort),
		)
}

func logHostPortStats(stats map[string]int, what string) {
	for hostPort, conns := range stats {
		if conns < connsThreshold {
			continue
		}
		logutil.Info("conntrack: "+what,
			zap.Any("host port", hostPort),
			zap.Any("connections", conns),
			zap.String("scope", "since_listener_start"),
			zap.Bool("complete", false),
		)
	}
}
