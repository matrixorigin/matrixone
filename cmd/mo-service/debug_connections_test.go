// Copyright 2022 Matrix Origin
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
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ti-mo/conntrack"
	"github.com/ti-mo/netfilter"
)

type failingConntrackClient struct {
	closed bool
}

func (c *failingConntrackClient) Close() error {
	c.closed = true
	return nil
}

func (*failingConntrackClient) Listen(chan<- conntrack.Event, uint8, []netfilter.NetlinkGroup) (chan error, error) {
	return nil, errors.New("listen failed")
}

func TestStartConnectionTrackingClosesOnListenFailure(t *testing.T) {
	originalDial := dialConntrack
	t.Cleanup(func() {
		dialConntrack = originalDial
	})

	client := new(failingConntrackClient)
	dialConntrack = func() (conntrackClient, error) {
		return client, nil
	}

	if err := startConnectionTracking(); err == nil {
		t.Fatal("expected Listen failure")
	}
	if !client.closed {
		t.Fatal("expected conntrack client to be closed after Listen failure")
	}
}

func TestDialConntrack(t *testing.T) {
	client, err := dialConntrack()
	if err != nil {
		t.Logf("conntrack unavailable in test environment: %v", err)
		return
	}
	if client == nil {
		t.Fatal("dialConntrack returned a nil client without an error")
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close conntrack client: %v", err)
	}
}

func TestConntrack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan conntrack.Event)
	errorCh := make(chan error)

	go logConnTrack(ctx, events, errorCh)

	t0 := time.Now()
	for {

		for range connsThreshold {

			events <- conntrack.Event{
				Type: conntrack.EventNew,
				Flow: &conntrack.Flow{
					ID: rand.Uint32(),
					TupleOrig: conntrack.Tuple{
						Proto: conntrack.ProtoTuple{
							SourcePort:      42,
							DestinationPort: 41,
						},
					},
				},
			}

			events <- conntrack.Event{
				Type: conntrack.EventDestroy,
				Flow: &conntrack.Flow{
					ID: rand.Uint32(),
					TupleOrig: conntrack.Tuple{
						Proto: conntrack.ProtoTuple{
							SourcePort:      42,
							DestinationPort: 41,
						},
					},
				},
			}

		}

		errorCh <- io.ErrShortBuffer
		events <- conntrack.Event{
			Type: conntrack.EventNew,
			Flow: &conntrack.Flow{
				ID: rand.Uint32(),
				TupleOrig: conntrack.Tuple{
					Proto: conntrack.ProtoTuple{
						SourcePort:      1,
						DestinationPort: 2,
					},
				},
			},
		}

		time.Sleep(time.Second)
		if time.Since(t0) > conntrackReportInterval+time.Second {
			break
		}
	}
}
