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

package main

import (
	"context"
	"io"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/ti-mo/conntrack"
)

func TestConntrack(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	events := make(chan conntrack.Event)
	errorCh := make(chan error)

	go logConnTrack(ctx, events, errorCh)

	t0 := time.Now()
	for {

		for range tooManyThreshold {

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
