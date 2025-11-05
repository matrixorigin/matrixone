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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/tidwall/btree"
)

// Reader lifecycle constants
const (
	DefaultFrequency     = 200 * time.Millisecond
	RetryableErrorPrefix = "retryable error:"
)

// WatermarkUpdater manages CDC watermarks
type WatermarkUpdater interface {
	RemoveCachedWM(ctx context.Context, key *WatermarkKey) (err error)
	UpdateWatermarkErrMsg(ctx context.Context, key *WatermarkKey, errMsg string) (err error)
	GetFromCache(ctx context.Context, key *WatermarkKey) (watermark types.TS, err error)
	GetOrAddCommitted(ctx context.Context, key *WatermarkKey, watermark *types.TS) (ret types.TS, err error)
	UpdateWatermarkOnly(ctx context.Context, key *WatermarkKey, watermark *types.TS) (err error)
}

const (
	CDCSourceUriPrefix = "mysql://"
	CDCSinkUriPrefix   = "mysql://"

	CDCState_Common = "common"
	CDCState_Error  = "error"
)

const (
	CDCDefaultSendSqlTimeout                 = "10m"
	CDCDefaultRetryTimes                     = -1
	CDCDefaultRetryDuration                  = 10 * time.Minute
	CDCDefaultTaskExtra_InitSnapshotSplitTxn = true
	CDCDefaultTaskExtra_MaxSQLLen            = 4 * 1024 * 1024
)

const (
	CDCSinkType_MySQL   = "mysql"
	CDCSinkType_MO      = "matrixone"
	CDCSinkType_Console = "console"
)

const (
	CDCPitrGranularity_Table   = "table"
	CDCPitrGranularity_DB      = "database"
	CDCPitrGranularity_Account = "account"
	CDCPitrGranularity_Cluster = "cluster"
	CDCPitrGranularity_All     = "*"
)

const (
	CDCRequestOptions_Level                = "Level"
	CDCRequestOptions_Exclude              = "Exclude"
	CDCRequestOptions_StartTs              = "StartTs"
	CDCRequestOptions_EndTs                = "EndTs"
	CDCRequestOptions_SendSqlTimeout       = "SendSqlTimeout"
	CDCRequestOptions_InitSnapshotSplitTxn = "InitSnapshotSplitTxn"
	CDCRequestOptions_MaxSqlLength         = "MaxSqlLength"
	CDCRequestOptions_NoFull               = "NoFull"
	CDCRequestOptions_ConfigFile           = "ConfigFile"
	CDCRequestOptions_Frequency            = "Frequency"
)

const (
	CDCTaskExtraOptions_MaxSqlLength         = CDCRequestOptions_MaxSqlLength
	CDCTaskExtraOptions_SendSqlTimeout       = CDCRequestOptions_SendSqlTimeout
	CDCTaskExtraOptions_InitSnapshotSplitTxn = CDCRequestOptions_InitSnapshotSplitTxn
	CDCTaskExtraOptions_Frequency            = CDCRequestOptions_Frequency
)

var CDCRequestOptions = []string{
	CDCRequestOptions_Level,
	CDCRequestOptions_Exclude,
	CDCRequestOptions_StartTs,
	CDCRequestOptions_EndTs,
	CDCRequestOptions_MaxSqlLength,
	CDCRequestOptions_SendSqlTimeout,
	CDCRequestOptions_InitSnapshotSplitTxn,
	CDCRequestOptions_ConfigFile,
	CDCRequestOptions_NoFull,
	CDCRequestOptions_Frequency,
}

var CDCTaskExtraOptions = []string{
	CDCTaskExtraOptions_MaxSqlLength,
	CDCTaskExtraOptions_SendSqlTimeout,
	CDCTaskExtraOptions_InitSnapshotSplitTxn,
	CDCTaskExtraOptions_Frequency,
}

var (
	EnableConsoleSink = false
)

type TaskId = uuid.UUID

func NewTaskId() TaskId {
	return uuid.Must(uuid.NewV7())
}

// func StringToTaskId(s string) (TaskId, error) {
// 	return uuid.Parse(s)
// }

type Reader interface {
	Run(ctx context.Context, ar *ActiveRoutine)
	Close()
}

type TableReader interface {
	Run(ctx context.Context, ar *ActiveRoutine)
	Close()
	Info() *DbTableInfo
	GetWg() *sync.WaitGroup
}

// Sinker manages and drains the sql parts
type Sinker interface {
	Run(ctx context.Context, ar *ActiveRoutine)
	Sink(ctx context.Context, data *DecoderOutput)
	SendBegin()
	SendCommit()
	SendRollback()
	// SendDummy to guarantee the last sql is sent
	SendDummy()
	// Error must be called after Sink
	Error() error
	ClearError()
	Reset()
	Close()
}

// Sink represents the destination mysql or matrixone
type Sink interface {
	Send(ctx context.Context, ar *ActiveRoutine, sqlBuf []byte, needRetry bool) error
	SendBegin(ctx context.Context) error
	SendCommit(ctx context.Context) error
	SendRollback(ctx context.Context) error
	Reset()
	Close()
}

type ActiveRoutine struct {
	sync.Mutex
	Pause  chan struct{}
	Cancel chan struct{}
}

func (ar *ActiveRoutine) ClosePause() {
	ar.Lock()
	defer ar.Unlock()
	close(ar.Pause)
	// can't set to nil, because some goroutines may still be running, when it goes next round loop,
	// it found the channel is nil, not closed, will hang there forever
}

func (ar *ActiveRoutine) CloseCancel() {
	ar.Lock()
	defer ar.Unlock()
	close(ar.Cancel)
}

func NewCdcActiveRoutine() *ActiveRoutine {
	return &ActiveRoutine{
		Pause:  make(chan struct{}),
		Cancel: make(chan struct{}),
	}
}

type OutputType int

const (
	OutputTypeSnapshot OutputType = iota
	OutputTypeTail
)

func (t OutputType) String() string {
	switch t {
	case OutputTypeSnapshot:
		return "Snapshot"
	case OutputTypeTail:
		return "Tail"
	default:
		return "usp output type"
	}
}

type DecoderOutput struct {
	outputTyp      OutputType
	noMoreData     bool
	fromTs, toTs   types.TS
	checkpointBat  *batch.Batch
	insertAtmBatch *AtomicBatch
	deleteAtmBatch *AtomicBatch
}

type RowType int

const (
	NoOp RowType = iota
	InsertRow
	DeleteRow
)

type RowIterator interface {
	Next() bool
	Row(ctx context.Context, row []any) error
	Close()
}

type DbTableInfo struct {
	SourceDbId      uint64
	SourceDbName    string
	SourceTblId     uint64
	SourceTblName   string
	SourceCreateSql string

	SinkDbName  string
	SinkTblName string

	IdChanged bool
}

func (info DbTableInfo) String() string {
	return fmt.Sprintf("%v(%v).%v(%v) -> %v.%v, %v",
		info.SourceDbName,
		info.SourceDbId,
		info.SourceTblName,
		info.SourceTblId,
		info.SinkDbName,
		info.SinkTblName,
		info.IdChanged,
	)
}

func (info DbTableInfo) Clone() *DbTableInfo {
	return &DbTableInfo{
		SourceDbId:      info.SourceDbId,
		SourceDbName:    info.SourceDbName,
		SourceTblId:     info.SourceTblId,
		SourceTblName:   info.SourceTblName,
		SourceCreateSql: info.SourceCreateSql,
		SinkDbName:      info.SinkDbName,
		SinkTblName:     info.SinkTblName,
		IdChanged:       info.IdChanged,
	}
}

func (info DbTableInfo) OnlyDiffinTblId(t *DbTableInfo) bool {
	if info.SourceDbId != t.SourceDbId ||
		info.SourceDbName != t.SourceDbName ||
		info.SourceTblName != t.SourceTblName ||
		info.SourceCreateSql != t.SourceCreateSql {
		return false
	}
	return info.SourceTblId != t.SourceTblId
}

// AtomicBatch holds batches from [Tail_wip,...,Tail_done] or [Tail_done].
// These batches have atomicity
type AtomicBatch struct {
	Mp      *mpool.MPool
	Batches []*batch.Batch
	Rows    *btree.BTreeG[AtomicBatchRow]
}

func NewAtomicBatch(mp *mpool.MPool) *AtomicBatch {
	opts := btree.Options{
		Degree: 64,
	}
	ret := &AtomicBatch{
		Mp:   mp,
		Rows: btree.NewBTreeGOptions(AtomicBatchRow.Less, opts),
	}
	return ret
}

type AtomicBatchRow struct {
	Ts     types.TS
	Pk     []byte
	Offset int
	Src    *batch.Batch
}

func (row AtomicBatchRow) Less(other AtomicBatchRow) bool {
	//ts asc
	if row.Ts.LT(&other.Ts) {
		return true
	}
	if row.Ts.GT(&other.Ts) {
		return false
	}
	//pk asc
	return bytes.Compare(row.Pk, other.Pk) < 0
}

func (bat *AtomicBatch) RowCount() int {
	c := 0
	for _, b := range bat.Batches {
		rows := 0
		if b != nil && len(b.Vecs) > 0 {
			rows = b.Vecs[0].Length()
		}
		c += rows
	}

	if c != bat.Rows.Len() {
		logutil.Errorf("inconsistent row count, sum rows of batches: %d, rows of btree: %d\n", c, bat.Rows.Len())
	}
	return c
}

func (bat *AtomicBatch) Allocated() int {
	size := 0
	for _, b := range bat.Batches {
		size += b.Allocated()
	}
	return size
}

func (bat *AtomicBatch) Append(
	packer *types.Packer,
	batch *batch.Batch,
	tsColIdx, compositedPkColIdx int,
) {
	start := time.Now()
	defer func() {
		v2.CdcAppendDurationHistogram.Observe(time.Since(start).Seconds())
	}()

	if batch != nil {
		//ts columns
		tsVec := vector.MustFixedColWithTypeCheck[types.TS](batch.Vecs[tsColIdx])
		//composited pk columns
		compositedPkBytes := readutil.EncodePrimaryKeyVector(batch.Vecs[compositedPkColIdx], packer)

		for i, pk := range compositedPkBytes {
			// if ts is constant, then tsVec[0] is the ts for all rows
			ts := tsVec[0]
			if i < len(tsVec) {
				ts = tsVec[i]
			}

			row := AtomicBatchRow{
				Ts:     ts,
				Pk:     pk,
				Offset: i,
				Src:    batch,
			}
			bat.Rows.Set(row)
		}

		bat.Batches = append(bat.Batches, batch)
	}
}

func (bat *AtomicBatch) Close() {
	for _, oneBat := range bat.Batches {
		oneBat.Clean(bat.Mp)
	}
	if bat.Rows != nil {
		bat.Rows.Clear()
		bat.Rows = nil
	}
	bat.Batches = nil
	bat.Mp = nil
}

func (bat *AtomicBatch) GetRowIterator() RowIterator {
	return &atomicBatchRowIter{
		iter: bat.Rows.Iter(),
	}
}

//func (bat *AtomicBatch) DebugString(tableDef *plan.TableDef, isDelete bool) string {
//	ctx := context.Background()
//	keys := make([]string, 0, bat.Rows.Len())
//	iter := bat.Rows.Iter()
//	defer iter.Release()
//	for iter.Next() {
//		row := iter.Item()
//		s, err := getRowPkAndTsFromBat(ctx, row.Src, tableDef, isDelete, row.Offset)
//		if err != nil {
//			return ""
//		}
//		keys = append(keys, s)
//	}
//	return fmt.Sprintf("count=%d, key=%v", bat.Rows.Len(), keys)
//}

var _ RowIterator = new(atomicBatchRowIter)

type atomicBatchRowIter struct {
	iter btree.IterG[AtomicBatchRow]
}

func (iter *atomicBatchRowIter) Item() AtomicBatchRow {
	return iter.iter.Item()
}

func (iter *atomicBatchRowIter) Next() bool {
	return iter.iter.Next()
}

func (iter *atomicBatchRowIter) Row(ctx context.Context, row []any) error {
	batchRow := iter.iter.Item()
	return extractRowFromEveryVector(
		ctx,
		batchRow.Src,
		batchRow.Offset,
		row,
	)
}

func (iter *atomicBatchRowIter) Close() {
	iter.iter.Release()
}

type UriInfo struct {
	SinkTyp       string `json:"_"`
	User          string `json:"user"`
	Password      string `json:"-"`
	Ip            string `json:"ip"`
	Port          int    `json:"port"`
	PasswordStart int    `json:"-"`
	PasswordEnd   int    `json:"-"`
	Reserved      string `json:"reserved"`
}

func (info *UriInfo) GetEncodedPassword() (string, error) {
	return AesCFBEncode([]byte(info.Password))
}

func (info *UriInfo) String() string {
	return fmt.Sprintf("%s%s:%s@%s:%d", CDCSourceUriPrefix, info.User, "******", info.Ip, info.Port)
}

type PatternTable struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

func (table PatternTable) String() string {
	return fmt.Sprintf("%s.%s", table.Database, table.Table)
}

type PatternTuple struct {
	Source       PatternTable `json:"Source"`
	Sink         PatternTable `json:"Sink"`
	OriginString string       `json:"-"`
	Reserved     string       `json:"reserved"`
}

func (tuple *PatternTuple) String() string {
	if tuple == nil {
		return ""
	}
	return fmt.Sprintf("%s,%s", tuple.Source, tuple.Sink)
}

type PatternTuples struct {
	Pts      []*PatternTuple `json:"pts"`
	Reserved string          `json:"reserved"`
}

func (pts *PatternTuples) Append(pt *PatternTuple) {
	pts.Pts = append(pts.Pts, pt)
}

func (pts *PatternTuples) String() string {
	if pts.Pts == nil {
		return ""
	}
	ss := make([]string, 0)
	for _, pt := range pts.Pts {
		ss = append(ss, pt.String())
	}
	return strings.Join(ss, ",")
}

// JsonEncode encodes the object to json
func JsonEncode(value any) (string, error) {
	jbytes, err := json.Marshal(value)
	if err != nil {
		return "", err
	}

	return hex.EncodeToString(jbytes), nil
}

// JsonDecode decodes the json bytes to objects
func JsonDecode(jbytes string, value any) error {
	jRawBytes, err := hex.DecodeString(jbytes)
	if err != nil {
		return err
	}

	err = json.Unmarshal(jRawBytes, value)
	if err != nil {
		return err
	}
	return nil
}
