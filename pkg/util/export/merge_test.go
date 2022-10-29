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
	"errors"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func init() {
	time.Local = time.FixedZone("CST", 0) // set time-zone +0000
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
