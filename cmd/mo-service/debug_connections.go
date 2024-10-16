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
	"cmp"
	"context"
	"maps"
	"runtime"
	"slices"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/ti-mo/conntrack"
	"github.com/ti-mo/netfilter"
	"go.uber.org/zap"
)

func init() {
	startConnectionTracking()
}

const (
	tooManyThreshold = 4096
)

var (
	conntrackReportInterval = time.Second * 5
)

func startConnectionTracking() (err error) {
	defer func() {
		if err != nil {
			logutil.Error("conntrack: error", zap.Error(err))
		}
	}()

	c, err := conntrack.Dial(nil)
	if err != nil {
		return err
	}

	events := make(chan conntrack.Event, 65536)
	errorChan, err := c.Listen(events, uint8(max(runtime.NumCPU(), 16)), []netfilter.NetlinkGroup{
		netfilter.GroupCTNew,
		netfilter.GroupCTDestroy,
	})
	if err != nil {
		return err
	}

	go func() {
		defer c.Close()
		logConnTrack(context.Background(), events, errorChan)
	}()

	return nil
}

func logConnTrack(ctx context.Context, events chan conntrack.Event, errorChan chan error) {

	activeConns := make(map[uint32]*conntrack.Flow)
	ticker := time.NewTicker(conntrackReportInterval)

	for {
		select {

		case <-ctx.Done():
			return

		case ev := <-events:
			switch ev.Type {

			case conntrack.EventNew:
				activeConns[ev.Flow.ID] = ev.Flow

			case conntrack.EventDestroy:
				delete(activeConns, ev.Flow.ID)

			}

		case err := <-errorChan:
			logutil.Error("conntrack: error", zap.Error(err))

		case <-ticker.C:
			logutil.Info("conntrack: stats",
				zap.Any("connections", len(activeConns)),
			)

			if len(activeConns) > tooManyThreshold {
				logConnectionStats(activeConns)
			}

		}
	}
}

func logConnectionStats(actives map[uint32]*conntrack.Flow) {
	srcPorts := make(map[uint16]int)
	destPorts := make(map[uint16]int)
	for _, flow := range actives {
		srcPorts[flow.TupleOrig.Proto.SourcePort]++
		destPorts[flow.TupleOrig.Proto.DestinationPort]++
	}
	logPortStats(srcPorts, "source port")
	logPortStats(destPorts, "dest port")
}

func logPortStats(stats map[uint16]int, what string) {
	srcKeys := slices.SortedFunc(maps.Keys(stats), func(a, b uint16) int {
		return -cmp.Compare(stats[a], stats[b])
	})
	for _, port := range srcKeys {
		if stats[port] < tooManyThreshold/10 {
			break
		}
		logutil.Info("conntrack: port stats",
			zap.Any(what, port),
			zap.Any("connections", stats[port]),
		)
	}
}
