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

package export

import (
	"context"
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"path"
	"strings"
	"testing"
	"time"
)

func init() {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
	RegisterTableDefine(dummyTable)
}

func TestInitCronExpr(t *testing.T) {
	type args struct {
		duration time.Duration
	}
	tests := []struct {
		name           string
		args           args
		wantErr        bool
		wantExpr       string
		expectDuration time.Duration
	}{
		{name: "1h", args: args{duration: 1 * time.Hour}, wantErr: false, wantExpr: MergeTaskCronExprEvery1Hour},
		{name: "2h", args: args{duration: 2 * time.Hour}, wantErr: false, wantExpr: MergeTaskCronExprEvery2Hour},
		{name: "4h", args: args{duration: 4 * time.Hour}, wantErr: false, wantExpr: MergeTaskCronExprEvery4Hour},
		{name: "3h", args: args{duration: 3 * time.Hour}, wantErr: false, wantExpr: "0 0 3,6,9,12,15,18,21 * * *"},
		{name: "5h", args: args{duration: 5 * time.Hour}, wantErr: false, wantExpr: "0 0 5,10,15,20 * * *"},
		{name: "5min", args: args{duration: 5 * time.Minute}, wantErr: false, wantExpr: MergeTaskCronExprEvery05Min},
		{name: "15min", args: args{duration: 15 * time.Minute}, wantErr: false, wantExpr: MergeTaskCronExprEvery15Min},
		{name: "7min", args: args{duration: 7 * time.Minute}, wantErr: false, wantExpr: "@every 10m", expectDuration: 10 * time.Minute},
		{name: "15s", args: args{duration: 15 * time.Second}, wantErr: false, wantExpr: "@every 15s", expectDuration: 15 * time.Second},
		{name: "2min", args: args{duration: 2 * time.Minute}, wantErr: false, wantExpr: "@every 120s", expectDuration: 2 * time.Minute},
		{name: "13h", args: args{duration: 13 * time.Hour}, wantErr: true, wantExpr: ""},
	}

	parser := cron.NewParser(
		cron.Second |
			cron.Minute |
			cron.Hour |
			cron.Dom |
			cron.Month |
			cron.Dow |
			cron.Descriptor)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := InitCronExpr(tt.args.duration)
			if tt.wantErr {
				var e *moerr.Error
				require.True(t, errors.As(err, &e))
				require.True(t, moerr.IsMoErrCode(e, moerr.ErrNotSupported))
			} else {
				require.Equal(t, tt.wantExpr, MergeTaskCronExpr)

				sche, err := parser.Parse(MergeTaskCronExpr)
				require.Nil(t, err)

				now := time.Unix(60, 0)
				next := sche.Next(time.UnixMilli(now.UnixMilli()))
				t.Logf("duration: %v, expr: %s, next: %v", tt.args.duration, MergeTaskCronExpr, next)
				if tt.expectDuration > 0 {
					require.Equal(t, tt.expectDuration, next.Sub(now))
				} else {
					require.Equal(t, tt.args.duration-time.Minute, next.Sub(now))
				}
			}
		})
	}
}

func initLogsFile(ctx context.Context, fs fileservice.FileService, table *Table, ts time.Time) error {
	mux.Lock()
	defer mux.Unlock()

	var newFilePath = func(ts time.Time) string {
		filename := table.PathBuilder.NewLogFilename(table.GetName(), "uuid", "node", ts)
		p := table.PathBuilder.Build(table.Account, MergeLogTypeLogs, ts, table.Database, table.GetName())
		filepath := path.Join(p, filename)
		return filepath
	}

	ts1 := ts
	writer, _ := NewCSVWriter(ctx, fs, newFilePath(ts1))
	writer.WriteStrings(dummyFillTable("row1", 1, 1.0).ToStrings())
	writer.WriteStrings(dummyFillTable("row2", 2, 2.0).ToStrings())
	writer.FlushAndClose()

	ts2 := ts.Add(time.Minute)
	writer, _ = NewCSVWriter(ctx, fs, newFilePath(ts2))
	writer.WriteStrings(dummyFillTable("row3", 1, 1.0).ToStrings())
	writer.WriteStrings(dummyFillTable("row4", 2, 2.0).ToStrings())
	writer.FlushAndClose()

	ts3 := ts.Add(time.Hour)
	writer, _ = NewCSVWriter(ctx, fs, newFilePath(ts3))
	writer.WriteStrings(dummyFillTable("row5", 1, 1.0).ToStrings())
	writer.WriteStrings(dummyFillTable("row6", 2, 2.0).ToStrings())
	writer.FlushAndClose()

	ts1New := ts.Add(time.Hour + time.Minute)
	writer, _ = NewCSVWriter(ctx, fs, newFilePath(ts1New))
	writer.WriteStrings(dummyFillTable("row1", 1, 11.0).ToStrings())
	writer.WriteStrings(dummyFillTable("row2", 2, 22.0).ToStrings())
	writer.FlushAndClose()

	return nil
}

func initSingleLogsFile(ctx context.Context, fs fileservice.FileService, table *Table, ts time.Time) error {
	mux.Lock()
	defer mux.Unlock()

	var newFilePath = func(ts time.Time) string {
		filename := table.PathBuilder.NewLogFilename(table.GetName(), "uuid", "node", ts)
		p := table.PathBuilder.Build(table.Account, MergeLogTypeLogs, ts, table.Database, table.GetName())
		filepath := path.Join(p, filename)
		return filepath
	}

	ts1 := ts
	writer, _ := NewCSVWriter(ctx, fs, newFilePath(ts1))
	writer.WriteStrings(dummyFillTable("row1", 1, 1.0).ToStrings())
	writer.WriteStrings(dummyFillTable("row2", 2, 2.0).ToStrings())
	writer.FlushAndClose()

	return nil
}

func TestNewMerge(t *testing.T) {
	fs, err := fileservice.NewLocalETLFS(etlFileServiceName, t.TempDir())
	require.Nil(t, err)
	ts, _ := time.Parse("2006-01-02 15:04:05", "2021-01-01 00:00:00")

	type args struct {
		ctx  context.Context
		opts []MergeOption
	}
	tests := []struct {
		name string
		args args
		want *Merge
	}{
		{
			name: "normal",
			args: args{
				ctx: context.Background(),
				opts: []MergeOption{WithFileServiceName(etlFileServiceName),
					WithFileService(fs), WithTable(dummyTable),
					WithMaxFileSize(1), WithMinFilesMerge(1), WithMaxFileSize(mpool.PB), WithMaxMergeJobs(16)},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			err := initLogsFile(tt.args.ctx, fs, dummyTable, ts)
			require.Nil(t, err)

			got := NewMerge(tt.args.ctx, tt.args.opts...)
			require.NotNil(t, got)

			err = got.Main(ts)
			require.Nilf(t, err, "err: %v", err)

			files := make([]string, 0, 1)
			dir := []string{"/"}
			for len(dir) > 0 {
				entrys, _ := fs.List(tt.args.ctx, dir[0])
				for _, e := range entrys {
					p := path.Join(dir[0], e.Name)
					if e.IsDir {
						dir = append(dir, p)
					} else {
						files = append(files, p)
					}
				}
				dir = dir[1:]
			}
			require.Equal(t, 1, len(files))
			t.Logf("%v", files)

			r, err := NewCSVReader(tt.args.ctx, fs, files[0])
			require.Nil(t, err)
			lines := 0
			for l := r.ReadLine(); l != nil; l = r.ReadLine() {
				lines++
				t.Logf("line %d: %s", lines, l)
			}
			require.Equal(t, 6, lines)

		})
	}
}

func TestMergeTaskExecutorFactory(t *testing.T) {
	t.Logf("tmpDir: %s/%s", t.TempDir(), t.Name())
	fs, err := fileservice.NewLocalETLFS(etlFileServiceName, path.Join(t.TempDir(), t.Name()))
	require.Nil(t, err)
	targetDate := "2021-01-01"
	ts, err := time.Parse("2006-01-02 15:04:05", targetDate+" 00:00:00")
	require.Nil(t, err)

	type args struct {
		ctx  context.Context
		opts []MergeOption
		task task.Task
	}
	tests := []struct {
		name string
		args args
		want func(ctx context.Context, task task.Task) error
	}{
		{
			name: "normal",
			args: args{
				ctx:  context.Background(),
				opts: []MergeOption{WithFileService(fs), WithMinFilesMerge(1)},
				task: task.Task{
					Metadata: task.TaskMetadata{
						ID:                   "",
						Executor:             0,
						Context:              []byte(strings.Join([]string{dummyTable.GetIdentify(), targetDate}, ParamSeparator)),
						Options:              task.TaskOptions{},
						XXX_NoUnkeyedLiteral: struct{}{},
						XXX_unrecognized:     nil,
						XXX_sizecache:        0,
					},
				},
			},
			want: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			err := initSingleLogsFile(tt.args.ctx, fs, dummyTable, ts)
			require.Nil(t, err)

			got := MergeTaskExecutorFactory(tt.args.opts...)
			require.NotNil(t, got)

			err = got(tt.args.ctx, tt.args.task)
			require.Nilf(t, err, "err: %v", err)

			files := make([]string, 0, 1)
			dir := []string{"/"}
			for len(dir) > 0 {
				entrys, _ := fs.List(tt.args.ctx, dir[0])
				for _, e := range entrys {
					p := path.Join(dir[0], e.Name)
					if e.IsDir {
						dir = append(dir, p)
					} else {
						files = append(files, p)
					}
				}
				dir = dir[1:]
			}
			require.Equal(t, 1, len(files))
			t.Logf("%v", files)
		})
	}
}

func TestCreateCronTask(t *testing.T) {
	store := taskservice.NewMemTaskStorage()
	s := taskservice.NewTaskService(store, nil)
	defer func() {
		assert.NoError(t, s.Close())
	}()
	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()

	type args struct {
		ctx         context.Context
		executorID  task.TaskCode
		taskService taskservice.TaskService
	}
	tests := []struct {
		name    string
		args    args
		wantErr error
	}{
		{
			name: "name",
			args: args{
				ctx:         ctx,
				executorID:  1,
				taskService: s,
			},
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CreateCronTask(tt.args.ctx, tt.args.executorID, tt.args.taskService)
			require.Nil(t, got)
		})
	}
}

func TestNewMergeService(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), time.Minute*5)
	defer cancel()
	fs, err := fileservice.NewLocalETLFS(etlFileServiceName, path.Join(t.TempDir(), t.Name()))
	require.Nil(t, err)

	type args struct {
		ctx  context.Context
		opts []MergeOption
	}
	tests := []struct {
		name  string
		args  args
		want  *Merge
		want1 bool
	}{
		{
			name: "normal",
			args: args{
				ctx:  ctx,
				opts: []MergeOption{WithFileService(fs), WithMinFilesMerge(1), WithTable(dummyTable)},
			},
			want:  nil,
			want1: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := NewMergeService(tt.args.ctx, tt.args.opts...)
			require.NotNil(t, got)
			require.Equal(t, tt.want1, got1)
		})
	}
}
