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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/tidwall/btree"
)

const (
	AccountLevel    = "account"
	ClusterLevel    = "cluster"
	MysqlSink       = "mysql"
	MatrixoneSink   = "matrixone"
	ConsoleSink     = "console"
	SourceUriPrefix = "mysql://"
	SinkUriPrefix   = "mysql://"
	ConsolePrefix   = "console://" //only used in testing stage

	SASCommon = "common"
	SASError  = "error"
)

var (
	EnableConsoleSink = false
)

type Reader interface {
	Run(ctx context.Context, ar *ActiveRoutine)
	Close()
}

// Sinker manages and drains the sql parts
type Sinker interface {
	Sink(ctx context.Context, data *DecoderOutput) error
}

// Sink represents the destination mysql or matrixone
type Sink interface {
	Send(ctx context.Context, ar *ActiveRoutine, sql string) error
	Close()
}

type ActiveRoutine struct {
	Cancel chan struct{}
}

func NewCdcActiveRoutine() *ActiveRoutine {
	activeRoutine := &ActiveRoutine{}
	activeRoutine.Cancel = make(chan struct{})
	return activeRoutine
}

type OutputType int

const (
	OutputTypeCheckpoint OutputType = iota
	OutputTypeTailDone
	OutputTypeUnfinishedTailWIP
)

func (t OutputType) String() string {
	switch t {
	case OutputTypeCheckpoint:
		return "Checkpoint"
	case OutputTypeTailDone:
		return "TailDone"
	case OutputTypeUnfinishedTailWIP:
		return "UnfinishedTailWIP"
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
	Close() error
}

type DbTableInfo struct {
	SourceAccountName string
	SourceDbName      string
	SourceTblName     string
	SourceAccountId   uint64
	SourceDbId        uint64
	SourceTblId       uint64

	SinkAccountName string
	SinkDbName      string
	SinkTblName     string
}

func (info DbTableInfo) String() string {
	return fmt.Sprintf("%v(%v).%v(%v) -> %v.%v",
		info.SourceDbName,
		info.SourceDbId,
		info.SourceTblName,
		info.SourceTblId,
		info.SinkDbName,
		info.SinkTblName,
	)
}

// AtomicBatch holds batches from [Tail_wip,...,Tail_done] or [Tail_done].
// These batches have atomicity
type AtomicBatch struct {
	Mp       *mpool.MPool
	From, To types.TS
	Batches  []*batch.Batch
	Rows     *btree.BTreeG[AtomicBatchRow]
}

func NewAtomicBatch(
	mp *mpool.MPool,
	from, to types.TS,
) *AtomicBatch {
	opts := btree.Options{
		Degree: 64,
	}
	ret := &AtomicBatch{
		Mp:   mp,
		From: from,
		To:   to,
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
	if batch != nil {
		//ts columns
		tsVec := vector.MustFixedColWithTypeCheck[types.TS](batch.Vecs[tsColIdx])
		//composited pk columns
		compositedPkBytes := logtailreplay.EncodePrimaryKeyVector(batch.Vecs[compositedPkColIdx], packer)

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
	count := float64(bat.RowCount())
	allocated := float64(bat.Allocated())
	v2.CdcTotalProcessingRecordCountGauge.Sub(count)
	v2.CdcTotalAllocatedBatchBytesGauge.Sub(allocated)
	v2.CdcSinkRecordCounter.Add(count)

	for _, oneBat := range bat.Batches {
		oneBat.Clean(bat.Mp)
	}
	bat.Rows.Clear()
	bat.Rows = nil
	bat.Batches = nil
	bat.Mp = nil
}

func (bat *AtomicBatch) GetRowIterator() RowIterator {
	return &atomicBatchRowIter{
		bat:      bat,
		iter:     bat.Rows.Iter(),
		initIter: bat.Rows.Iter(),
	}
}

var _ RowIterator = new(atomicBatchRowIter)

type atomicBatchRowIter struct {
	bat      *AtomicBatch
	iter     btree.IterG[AtomicBatchRow]
	initIter btree.IterG[AtomicBatchRow]
}

func (iter *atomicBatchRowIter) Item() AtomicBatchRow {
	return iter.iter.Item()
}

func (iter *atomicBatchRowIter) Reset() {
	iter.iter = iter.initIter
}

func (iter *atomicBatchRowIter) Prev() bool {
	return iter.iter.Prev()
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

func (iter *atomicBatchRowIter) Close() error {
	iter.iter.Release()
	iter.initIter.Release()
	iter.bat.Close()
	return nil
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
	return fmt.Sprintf("%s%s:%s@%s:%d", SourceUriPrefix, info.User, "******", info.Ip, info.Port)
}

type PatternTable struct {
	AccountId     uint64 `json:"account_id"`
	Account       string `json:"account"`
	Database      string `json:"database"`
	Table         string `json:"table"`
	TableIsRegexp bool   `json:"table_is_regexp"`
	Reserved      bool   `json:"reserved"`
}

func (table PatternTable) String() string {
	return fmt.Sprintf("(%s,%d,%s,%s)", table.Account, table.AccountId, table.Database, table.Table)
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
