// Copyright 2024 Matrix Origin
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

package etl

import (
	"bytes"
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	db_holder "github.com/matrixorigin/matrixone/pkg/util/export/etl/db"
	"github.com/matrixorigin/matrixone/pkg/util/export/table"
)

var _ table.Flusher = (*dummyFlusher)(nil)

type dummyFlusher struct {
	buf *bytes.Buffer
}

func (f *dummyFlusher) FlushBuffer(buffer *bytes.Buffer) (int, error) {
	return f.buf.Write(buffer.Bytes())
}

func NewDummyFlusher() *dummyFlusher {
	return &dummyFlusher{buf: bytes.NewBuffer(make([]byte, 0, mpool.MB))}
}

func TestContentWriter_WriteRow_SqlFlushOK(t *testing.T) {
	ctx := context.TODO()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	mock.ExpectExec(regexp.QuoteMeta(`LOAD DATA INLINE FORMAT='csv', DATA='str,1,1,1,0001-01-01 00:00:00.000000,"{""key"": ""value""}"
' INTO TABLE db_dummy.tbl_all_type_dummy FIELDS TERMINATED BY ','`)).WillReturnResult(sqlmock.NewResult(1, 1))
	db_holder.SetDBConn(db)

	item := dummyItem{
		strVal:      "str",
		int64Val:    1,
		float64Val:  1.0,
		uint64Val:   1,
		datetimeVal: time.Time{},
		jsonVal:     `{"key": "value"}`,
	}
	tbl := item.GetTable()

	flusher := NewDummyFlusher()
	writer := NewContentWriter(ctx, tbl, flusher)
	writer.SetBuffer(bytes.NewBuffer(nil), nil)

	row := tbl.GetRow(ctx)
	item.FillRow(ctx, row)
	writer.WriteRow(row)
	writer.FlushAndClose()

	require.Equal(t, "", flusher.buf.String())
}

func TestContentWriter_WriteRow_SqlFlushNotOK(t *testing.T) {
	ctx := context.TODO()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	mock.ExpectExec(regexp.QuoteMeta(`LOAD DATA INLINE invalid sql`)).WillReturnResult(sqlmock.NewResult(1, 1))
	db_holder.SetDBConn(db)

	item := dummyItem{
		strVal:      "str",
		int64Val:    1,
		float64Val:  1.0,
		uint64Val:   1,
		datetimeVal: time.Time{},
		jsonVal:     `{"key": "value"}`,
	}
	tbl := item.GetTable()

	flusher := NewDummyFlusher()
	writer := NewContentWriter(ctx, tbl, flusher)
	writer.SetBuffer(bytes.NewBuffer(nil), nil)

	row := tbl.GetRow(ctx)
	item.FillRow(ctx, row)
	writer.WriteRow(row)
	writer.FlushAndClose()

	require.Equal(t, `str,1,1,1,0001-01-01 00:00:00.000000,"{""key"": ""value""}"
`,
		flusher.buf.String())
}

func TestContentWriter_FlushBuffer_SqlNotOK(t *testing.T) {
	ctx := context.TODO()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()
	mock.ExpectExec(regexp.QuoteMeta(`LOAD DATA INLINE invalid sql`)).WillReturnResult(sqlmock.NewResult(1, 1))
	db_holder.SetDBConn(db)

	item := dummyItem{
		strVal:      "str",
		int64Val:    1,
		float64Val:  1.0,
		uint64Val:   1,
		datetimeVal: time.Time{},
		jsonVal:     `{"key": "value"}`,
	}
	tbl := item.GetTable()

	buf := bytes.NewBuffer(nil)
	buf.Write([]byte("hello world"))
	callbackDone := false
	callback := func(buf *bytes.Buffer) { callbackDone = true }

	flusher := NewDummyFlusher()
	writer := NewContentWriter(ctx, tbl, flusher)
	writer.SetBuffer(buf, callback)

	writer.FlushAndClose()

	require.Equal(t, `hello world`, flusher.buf.String())
	require.True(t, callbackDone)
}
