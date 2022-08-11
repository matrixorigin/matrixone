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

package mem

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/logservice"
	logpb "github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/stretchr/testify/assert"
)

func TestAppendLog(t *testing.T) {
	l := NewMemLog()

	n := 100
	for i := 0; i < n; i++ {
		lsn, err := l.Append(context.Background(), logpb.LogRecord{Data: []byte{byte(i)}})
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), lsn)
	}
}

func TestReadLog(t *testing.T) {
	l := NewMemLog()

	n := 100
	var logs []logpb.LogRecord
	for i := 0; i < n; i++ {
		log := logpb.LogRecord{Data: []byte{byte(i)}}
		lsn, err := l.Append(context.Background(), log)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), lsn)
		log.Lsn = logservice.Lsn(i + 1)
		logs = append(logs, log)
	}

	readed, lsn, err := l.Read(context.Background(), 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lsn)
	assert.Equal(t, logs, readed)

	readed, lsn, err = l.Read(context.Background(), logservice.Lsn(n-1), 0)
	assert.NoError(t, err)
	assert.Equal(t, logservice.Lsn(n-1), lsn)
	assert.Equal(t, logs[n-2:], readed)

	readed, lsn, err = l.Read(context.Background(), logservice.Lsn(n), 0)
	assert.NoError(t, err)
	assert.Equal(t, logservice.Lsn(n), lsn)
	assert.Equal(t, logs[n-1:], readed)

	readed, lsn, err = l.Read(context.Background(), logservice.Lsn(n+1), 0)
	assert.NoError(t, err)
	assert.Equal(t, logservice.Lsn(n+1), lsn)
	assert.Empty(t, readed)
}

func TestTruncateLog(t *testing.T) {
	l := NewMemLog()

	n := 100
	var logs []logpb.LogRecord
	for i := 0; i < n; i++ {
		log := logpb.LogRecord{Data: []byte{byte(i)}}
		lsn, err := l.Append(context.Background(), log)
		assert.NoError(t, err)
		assert.Equal(t, uint64(i+1), lsn)
		log.Lsn = logservice.Lsn(i + 1)
		logs = append(logs, log)
	}

	assert.NoError(t, l.Truncate(context.Background(), 10))
	readed, lsn, err := l.Read(context.Background(), 1, 0)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), lsn)
	assert.Equal(t, logs[10:], readed)
}
