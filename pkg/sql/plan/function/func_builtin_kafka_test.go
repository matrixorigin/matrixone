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

package function

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// kafkaStateSession is a minimal process.Session + KafkaSessionState.
type kafkaStateSession struct {
	id  int64
	set bool
}

func (s *kafkaStateSession) GetTempTable(dbName, alias string) (string, bool) { return "", false }
func (s *kafkaStateSession) AddTempTable(dbName, alias, realName string)      {}
func (s *kafkaStateSession) RemoveTempTable(dbName, alias string)             {}
func (s *kafkaStateSession) RemoveTempTableByRealName(realName string)        {}
func (s *kafkaStateSession) GetSqlModeNoAutoValueOnZero() (bool, bool)        { return false, false }
func (s *kafkaStateSession) SetLastKafkaMessageID(id int64)                   { s.id, s.set = id, true }
func (s *kafkaStateSession) LastKafkaMessageID() (int64, bool)                { return s.id, s.set }

// TestLastKafkaMessageID drives the builtin through the registered overload:
// NULL before any scan (or without kafka session state), the recorded id
// after.
func TestLastKafkaMessageID(t *testing.T) {
	proc := testutil.NewProcess(t)

	fid, err := GetFunctionByName(context.Background(), "last_kafka_message_id", nil)
	require.NoError(t, err)

	run := func(proc *process.Process) *vector.Vector {
		v, err := RunFunctionDirectly(proc, fid.GetEncodedOverloadID(), nil, 1)
		require.NoError(t, err)
		return v
	}

	// no session at all -> NULL
	v := run(proc)
	require.True(t, v.GetNulls().Contains(0))
	v.Free(proc.Mp())

	// session state present but nothing recorded -> NULL
	ses := &kafkaStateSession{}
	proc.Session = ses
	v = run(proc)
	require.True(t, v.GetNulls().Contains(0))
	v.Free(proc.Mp())

	// a completed scan recorded an id -> the id
	ses.SetLastKafkaMessageID(12345)
	v = run(proc)
	require.False(t, v.GetNulls().Contains(0))
	require.Equal(t, int64(12345), vector.MustFixedColWithTypeCheck[int64](v)[0])
	require.Equal(t, types.T_int64, v.GetType().Oid)
	v.Free(proc.Mp())
}
