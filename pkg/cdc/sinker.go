// Copyright 2024 Matrix Origin
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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"go.uber.org/zap"
)

var NewSinker = func(
	sinkUri UriInfo,
	accountId uint64,
	taskId string,
	dbTblInfo *DbTableInfo,
	watermarkUpdater *CDCWatermarkUpdater,
	tableDef *plan.TableDef,
	retryTimes int,
	retryDuration time.Duration,
	ar *ActiveRoutine,
	maxSqlLength uint64,
	sendSqlTimeout string,
) (Sinker, error) {
	//TODO: remove console
	if sinkUri.SinkTyp == CDCSinkType_Console {
		return NewConsoleSinker(dbTblInfo, watermarkUpdater), nil
	}

	// Use the new v2 architecture
	return CreateMysqlSinker2(
		sinkUri,
		accountId,
		taskId,
		dbTblInfo,
		watermarkUpdater,
		tableDef,
		retryTimes,
		retryDuration,
		ar,
		maxSqlLength,
		sendSqlTimeout,
	)
}

var _ Sinker = new(consoleSinker)

type consoleSinker struct {
	dbTblInfo        *DbTableInfo
	watermarkUpdater *CDCWatermarkUpdater
}

func NewConsoleSinker(
	dbTblInfo *DbTableInfo,
	watermarkUpdater *CDCWatermarkUpdater,
) Sinker {
	return &consoleSinker{
		dbTblInfo:        dbTblInfo,
		watermarkUpdater: watermarkUpdater,
	}
}

func (s *consoleSinker) Run(_ context.Context, _ *ActiveRoutine) {}

func (s *consoleSinker) Sink(ctx context.Context, data *DecoderOutput) {
	tableName := ""
	if s.dbTblInfo != nil {
		tableName = s.dbTblInfo.String()
	}
	logutil.Info("cdc.sinker.console", zap.String("table", tableName))

	logutil.Info("cdc.sinker.console.output_type", zap.String("type", data.outputTyp.String()))
	switch data.outputTyp {
	case OutputTypeSnapshot:
		if data.checkpointBat != nil && data.checkpointBat.RowCount() > 0 {
			logutil.Info("cdc.sinker.console.checkpoint")
			//logutil.Info(data.checkpointBat.String())
		}
	case OutputTypeTail:
		if data.insertAtmBatch != nil && data.insertAtmBatch.Rows.Len() > 0 {
			wantedColCnt := len(data.insertAtmBatch.Batches[0].Vecs) - 2
			row := make([]any, wantedColCnt)
			wantedColIndice := make([]int, wantedColCnt)
			for i := 0; i < wantedColCnt; i++ {
				wantedColIndice[i] = i
			}

			iter := data.insertAtmBatch.GetRowIterator()
			for iter.Next() {
				_ = iter.Row(ctx, row)
				logutil.Info("cdc.sinker.console.insert_row",
					zap.Any("row", row),
				)
			}
			iter.Close()
		}
	}
}

func (s *consoleSinker) SendBegin() {}

func (s *consoleSinker) SendCommit() {}

func (s *consoleSinker) SendRollback() {}

func (s *consoleSinker) SendDummy() {}

func (s *consoleSinker) Error() error {
	return nil
}

func (s *consoleSinker) ClearError() {}

func (s *consoleSinker) Reset() {}

func (s *consoleSinker) Close() {}

//type matrixoneSink struct {
//}
//
//func (*matrixoneSink) Send(ctx context.Context, data *DecoderOutput) error {
//	return nil
//}
