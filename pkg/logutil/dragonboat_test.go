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

package logutil

import (
	"fmt"
	"testing"

	"github.com/lni/dragonboat/v4/logger"
	"github.com/stretchr/testify/require"
)

func TestDragonboatFactory(t *testing.T) {
	type args struct {
		name string

		formatter string
		args      []any
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{
				name:      "Test",
				formatter: "hello %s",
				args:      []any{"world"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			plog := logger.GetLogger(tt.args.name)
			plog.SetLevel(logger.DEBUG)
			plog.Debugf(tt.args.formatter, tt.args.args...)
			plog.Infof(tt.args.formatter, tt.args.args...)
			plog.Warningf(tt.args.formatter, tt.args.args...)
			plog.Errorf(tt.args.formatter, tt.args.args...)
			plog.SetLevel(logger.INFO)
			plog.SetLevel(logger.WARNING)
			plog.SetLevel(logger.ERROR)
			plog.SetLevel(logger.CRITICAL)

		})
	}
}

func TestDragonboat_panic(t *testing.T) {
	type args struct {
		name string

		formatter string
		args      []any
	}
	tests := []struct {
		name string
		args args
	}{
		{
			name: "normal",
			args: args{
				name:      "Test",
				formatter: "hello %s",
				args:      []any{"world"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				err := recover()
				require.Equal(t, fmt.Sprintf(tt.args.formatter, tt.args.args...), fmt.Sprintf("%s", err))
			}()

			plog := logger.GetLogger(tt.args.name)
			plog.SetLevel(logger.DEBUG)
			plog.Panicf(tt.args.formatter, tt.args.args...)

		})
	}
}
