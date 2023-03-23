// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package service

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	// FIXME: do we need to make event buffer size configurable?
	eventBufferSize = 6 * 1024
)

// Notifier provides incremental logtail.
type Notifier struct {
	ctx context.Context
	C   chan event
}

func NewNotifier(ctx context.Context, buffer int) *Notifier {
	return &Notifier{
		ctx: ctx,
		C:   make(chan event, buffer),
	}
}

// NotifyLogtail provides incremental logtail.
func (n *Notifier) NotifyLogtail(
	from, to timestamp.Timestamp, tails ...logtail.TableLogtail,
) error {
	select {
	case <-n.ctx.Done():
		return n.ctx.Err()
	case n.C <- event{from: from, to: to, logtails: tails}:
	}
	return nil
}

type event struct {
	from, to timestamp.Timestamp
	logtails []logtail.TableLogtail
}
