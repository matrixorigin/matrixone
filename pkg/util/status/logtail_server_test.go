// Copyright 2021 -2023 Matrix Origin
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

package status

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/logtail"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	taelogtail "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail/service"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type logtailer struct {
	tables []api.TableID
}

func mockLocktailer(tables ...api.TableID) taelogtail.Logtailer {
	return &logtailer{
		tables: tables,
	}
}

func mockLogtail(table api.TableID, ts timestamp.Timestamp) logtail.TableLogtail {
	return logtail.TableLogtail{
		CkpLocation: "checkpoint",
		Table:       &table,
		Ts:          &ts,
	}
}

func (m *logtailer) RangeLogtail(
	ctx context.Context, from, to timestamp.Timestamp,
) ([]logtail.TableLogtail, []func(), error) {
	tails := make([]logtail.TableLogtail, 0, len(m.tables))
	for _, table := range m.tables {
		tails = append(tails, mockLogtail(table, to))
	}
	return tails, nil, nil
}

func (m *logtailer) RegisterCallback(cb func(from, to timestamp.Timestamp, closeCB func(), tails ...logtail.TableLogtail) error) {
}

func (m *logtailer) TableLogtail(
	ctx context.Context, table api.TableID, from, to timestamp.Timestamp,
) (logtail.TableLogtail, func(), error) {
	for _, t := range m.tables {
		if t.String() == table.String() {
			return mockLogtail(table, to), nil, nil
		}
	}
	return logtail.TableLogtail{CkpLocation: "checkpoint", Table: &table, Ts: &to}, nil, nil
}

func (m *logtailer) Now() (timestamp.Timestamp, timestamp.Timestamp) {
	panic("not implemented")
}

func TestFillLogtail(t *testing.T) {
	var status Status
	rt := runtime.DefaultRuntime()
	logtailer := mockLocktailer()
	logtailServer, err := service.NewLogtailServer(
		"", options.NewDefaultLogtailServerCfg(), logtailer, rt,
		service.WithServerCollectInterval(20*time.Millisecond),
		service.WithServerSendTimeout(5*time.Second),
		service.WithServerEnableChecksum(true),
		service.WithServerMaxMessageSize(32+7),
	)
	require.NoError(t, err)
	assert.NoError(t, err)
	mgr := logtailServer.SessionMgr()
	for i := 0; i < 10; i++ {
		mgr.AddSession(uint64(i))
	}
	for i := 0; i < 10; i++ {
		mgr.AddDeletedSession(uint64(i))
	}
	status.LogtailServerStatus.fill(logtailServer)
	assert.Equal(t, 10, len(status.LogtailServerStatus.Sessions))
	assert.Equal(t, 10, len(status.LogtailServerStatus.DeletedSessions))
}
