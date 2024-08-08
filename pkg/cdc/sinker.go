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

package cdc

import (
	"context"
	"fmt"
	"os"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
)

var _ Sinker = new(consoleSinker)

type consoleSinker struct{}

func NewConsoleSinker() Sinker {
	return &consoleSinker{}
}

func (s *consoleSinker) Sink(ctx context.Context, cdcCtx *disttae.TableCtx, data *DecoderOutput) error {
	fmt.Fprintln(os.Stderr, "====console sinker====")
	fmt.Fprintln(os.Stderr, cdcCtx.Db(), cdcCtx.DBId(), cdcCtx.Table(), cdcCtx.TableId(), data.ts)
	if value, ok := data.sqlOfRows.Load().([][]byte); !ok {
		fmt.Fprintln(os.Stderr, "no sqlOfrows")
	} else {
		fmt.Fprintln(os.Stderr, "total rows sql", len(value))
		for i, sqlBytes := range value {
			fmt.Fprintln(os.Stderr, i, string(sqlBytes))
		}
	}

	return nil
}

type mysqlSink struct {
}

func (*mysqlSink) Send(ctx context.Context, data *DecoderOutput) error {
	return nil
}

type matrixoneSink struct {
}

func (*matrixoneSink) Send(ctx context.Context, data *DecoderOutput) error {
	return nil
}
