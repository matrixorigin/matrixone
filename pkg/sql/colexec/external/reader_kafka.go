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

package external

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util/csvparser"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// kafkaMetaTimeLayout renders a message timestamp so the shared CSV field
// conversion parses it back at millisecond precision in the session zone.
const kafkaMetaTimeLayout = "2006-01-02 15:04:05.000"

// KafkaExternParam builds the synthetic tree.ExternParam of a Kafka external
// table scan: an INLINE (single virtual file) read whose record format is the
// table's csv or jsonl option. CSV uses plain quoting with no backslash
// escaping (EscapedBy 0) — one Kafka message value is one record.
func KafkaExternParam(ks *plan.KafkaScan) *tree.ExternParam {
	tail := new(tree.TailParameter)
	format := tree.CSV
	jsonData := ""
	if ks.GetFormat() == sqlkafka.FormatJSONL {
		format = tree.JSONLINE
		jsonData = tree.OBJECT
	} else {
		tail.Fields = &tree.Fields{
			Terminated: &tree.Terminated{Value: ks.GetSeparator()},
			EscapedBy:  &tree.EscapedBy{Value: 0},
		}
	}
	return &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			ScanType: tree.INLINE,
			Format:   format,
			Tail:     tail,
		},
		ExParam: tree.ExParam{
			ExternType: int32(plan.ExternType_KAFKA_TB),
			JsonData:   jsonData,
		},
	}
}

// KafkaMsgMeta is the per-message metadata surfaced through the hidden
// __mo_message_* columns.
type KafkaMsgMeta struct {
	Offset int64
	Ts     time.Time
	Key    string
	NilKey bool
	Value  string
}

// KafkaMetaState synchronizes per-message metadata with the row conversion:
// the stream reader appends one entry per message it emits into the byte
// stream, and makeBatchRows pops one entry per record it converts. Everything
// runs on the scan goroutine — no locking. A Kafka message must parse to
// exactly one record; the counts are re-checked when the stream ends.
type KafkaMetaState struct {
	pending []KafkaMsgMeta
	cur     KafkaMsgMeta
	rows    int64
	msgs    int64
	loc     *time.Location
	// effective read controls, surfaced through the __mo_read_* columns
	startID int64
	size    int64
	timeout int64
}

// advance pops the metadata of the next record's message.
func (m *KafkaMetaState) advance(ctx context.Context) error {
	if len(m.pending) == 0 {
		return moerr.NewInvalidInput(ctx,
			"kafka message did not parse to exactly one record (more records than messages)")
	}
	m.cur = m.pending[0]
	m.pending = m.pending[1:]
	m.rows++
	return nil
}

// finishCheck runs when the stream is exhausted: every message must have
// produced exactly one record.
func (m *KafkaMetaState) finishCheck(ctx context.Context) error {
	if m.rows != m.msgs {
		return moerr.NewInvalidInputf(ctx,
			"kafka read consumed %d messages but parsed %d records; every message value must be exactly one %s record",
			m.msgs, m.rows, "csv/jsonl")
	}
	return nil
}

// kafkaMetaField synthesizes the hidden-column field values of the current
// row's message. Returns ok=false for ordinary data columns.
func kafkaMetaField(colName string, param *ExternalParam) (csvparser.Field, bool) {
	m := param.KafkaMeta
	if m == nil {
		return csvparser.Field{}, false
	}
	switch colName {
	case catalog.KafkaMessageID:
		return csvparser.Field{Val: strconv.FormatInt(m.cur.Offset, 10)}, true
	case catalog.KafkaMessageTS:
		return csvparser.Field{Val: m.cur.Ts.In(m.loc).Format(kafkaMetaTimeLayout)}, true
	case catalog.KafkaMessageKey:
		if m.cur.NilKey {
			return csvparser.Field{IsNull: true}, true
		}
		return csvparser.Field{Val: m.cur.Key}, true
	case catalog.KafkaMessageValue:
		return csvparser.Field{Val: m.cur.Value}, true
	case catalog.KafkaReadStartID:
		return csvparser.Field{Val: strconv.FormatInt(m.startID, 10)}, true
	case catalog.KafkaReadSize:
		return csvparser.Field{Val: strconv.FormatInt(m.size, 10)}, true
	case catalog.KafkaReadTimeout:
		return csvparser.Field{Val: strconv.FormatInt(m.timeout, 10)}, true
	}
	return csvparser.Field{}, false
}

// isKafkaSyntheticAttr reports whether attr names a synthetic Kafka column of
// this scan (jsonline conversion must not demand it as a JSON key).
func isKafkaSyntheticAttr(param *ExternalParam, colName string) bool {
	if param == nil || param.KafkaScan == nil {
		return false
	}
	switch colName {
	case catalog.KafkaMessageID, catalog.KafkaMessageTS, catalog.KafkaMessageKey,
		catalog.KafkaMessageValue, catalog.KafkaReadStartID, catalog.KafkaReadSize,
		catalog.KafkaReadTimeout:
		return true
	}
	return false
}

// kafkaStreamReader adapts a Kafka partition read into the io.ReadCloser the
// shared CSV machinery consumes: each message value becomes one line, and its
// metadata is appended to the KafkaMetaState FIFO in the same order. The read
// ends at the size cap, or when no new message arrives within the timeout.
type kafkaStreamReader struct {
	cl        *kgo.Client
	meta      *KafkaMetaState
	topic     string
	partition int32
	buf       []byte
	done      bool
	timeout   time.Duration // 0 = block forever
	sizeCap   int64         // 0 = unlimited
	readCnt   int64
	last      int64 // offset of the last message emitted
	anyRead   bool
	ctx       context.Context
}

func (s *kafkaStreamReader) Read(p []byte) (int, error) {
	for {
		if len(s.buf) > 0 {
			n := copy(p, s.buf)
			s.buf = s.buf[n:]
			return n, nil
		}
		if s.done {
			return 0, io.EOF
		}
		if s.sizeCap > 0 && s.readCnt >= s.sizeCap {
			s.done = true
			return 0, io.EOF
		}
		pollCtx := s.ctx
		var cancel context.CancelFunc
		if s.timeout > 0 {
			pollCtx, cancel = context.WithTimeout(s.ctx, s.timeout)
		}
		fetches := s.cl.PollFetches(pollCtx)
		if cancel != nil {
			cancel()
		}
		var pollErr error
		timedOut := false
		fetches.EachError(func(t string, part int32, err error) {
			if errors.Is(err, context.DeadlineExceeded) {
				timedOut = true
				return
			}
			if errors.Is(err, context.Canceled) && s.ctx.Err() != nil {
				pollErr = s.ctx.Err()
				return
			}
			if pollErr == nil {
				pollErr = moerr.NewInternalErrorf(s.ctx, "kafka fetch %s[%d]: %v", t, part, err)
			}
		})
		if pollErr != nil {
			return 0, pollErr
		}
		got := false
		fetches.EachRecord(func(rec *kgo.Record) {
			if s.sizeCap > 0 && s.readCnt >= s.sizeCap {
				return
			}
			got = true
			s.readCnt++
			s.last = rec.Offset
			s.anyRead = true
			key := ""
			nilKey := rec.Key == nil
			if !nilKey {
				key = string(rec.Key)
			}
			value := string(rec.Value)
			s.meta.pending = append(s.meta.pending, KafkaMsgMeta{
				Offset: rec.Offset,
				Ts:     rec.Timestamp,
				Key:    key,
				NilKey: nilKey,
				Value:  value,
			})
			s.meta.msgs++
			// one message value = one record line; strip a trailing newline
			// the producer may have included so the line boundary stays 1:1
			line := strings.TrimRight(value, "\r\n")
			s.buf = append(s.buf, line...)
			s.buf = append(s.buf, '\n')
		})
		if !got {
			if timedOut {
				s.done = true
				return 0, io.EOF
			}
			if err := s.ctx.Err(); err != nil {
				return 0, err
			}
			// blocking mode (timeout 0) with an empty poll: poll again
		}
	}
}

func (s *kafkaStreamReader) Close() error {
	s.done = true
	return nil
}

// KafkaReader is the ExternalFileReader of a Kafka external table: it reads
// one topic partition from a resolved start offset, parses each message value
// as one csv/jsonl record with the shared CSV machinery, and surfaces message
// metadata through the hidden __mo_message_* columns.
type KafkaReader struct {
	csv    CsvReader
	stream *kafkaStreamReader
	cl     *kgo.Client
	ks     *plan.KafkaScan
	proc   *process.Process
	// completed is set only when ReadBatch drained the stream without error;
	// commit/session-id updates in Close depend on it (an aborted read must
	// change nothing).
	completed bool
}

func NewKafkaReader(param *ExternalParam) *KafkaReader {
	return &KafkaReader{}
}

func (r *KafkaReader) Open(param *ExternalParam, proc *process.Process) (fileEmpty bool, err error) {
	ks := param.KafkaScan
	if ks == nil {
		return false, moerr.NewInternalError(proc.Ctx, "kafka reader without scan metadata")
	}

	// Resolve the first offset to read. start_id is "the last consumed
	// offset": reading begins after it. -1 means earliest (inclusive) with
	// autocommit=false, latest with autocommit=true; 0 with autocommit=true
	// keeps the issue-specified "0 = earliest (inclusive)" meaning.
	var kOff kgo.Offset
	explicitAfter := int64(-1) // >=0: first offset to read is this value
	switch {
	case ks.HasStartId && ks.StartId == -1 && ks.Autocommit:
		kOff = kgo.NewOffset().AtEnd()
	case ks.HasStartId && ks.StartId == -1 && !ks.Autocommit:
		kOff = kgo.NewOffset().AtStart()
	case ks.HasStartId && ks.StartId == 0 && ks.Autocommit:
		kOff = kgo.NewOffset().AtStart()
	case ks.HasStartId:
		explicitAfter = ks.StartId + 1
		kOff = kgo.NewOffset().At(explicitAfter)
	default:
		// only reachable with autocommit=true (compile enforces the
		// requirement for autocommit=false): default 0 = earliest
		kOff = kgo.NewOffset().AtStart()
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(ks.Brokers, ",")...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			ks.Topic: {ks.Partition: kOff},
		}),
	)
	if err != nil {
		return false, moerr.NewInternalErrorf(proc.Ctx, "kafka: cannot create client for %s: %v", ks.Brokers, err)
	}

	// With autocommit=false the read position is committed BEFORE reading
	// (issue #27518): committing the same start twice is idempotent, so a
	// retried read with the same __mo_read_start_id returns the same data.
	if !ks.Autocommit {
		commitAt := explicitAfter
		if commitAt < 0 {
			// start from the earliest: commit the partition log start
			adm := kadm.NewClient(cl)
			listed, lerr := adm.ListStartOffsets(proc.Ctx, ks.Topic)
			if lerr != nil {
				cl.Close()
				return false, moerr.NewInternalErrorf(proc.Ctx, "kafka: cannot list start offsets: %v", lerr)
			}
			lo, ok := listed.Lookup(ks.Topic, ks.Partition)
			if !ok || lo.Err != nil {
				cl.Close()
				return false, moerr.NewInternalErrorf(proc.Ctx, "kafka: no start offset for %s[%d]", ks.Topic, ks.Partition)
			}
			commitAt = lo.Offset
		}
		if err := kafkaCommit(proc.Ctx, cl, ks, commitAt); err != nil {
			cl.Close()
			return false, err
		}
	}

	loc := time.Local
	if si := proc.GetSessionInfo(); si != nil && si.TimeZone != nil {
		loc = si.TimeZone
	}
	startID := int64(0)
	if ks.HasStartId {
		startID = ks.StartId
	}
	meta := &KafkaMetaState{
		loc:     loc,
		startID: startID,
		size:    ks.Size,
		timeout: ks.TimeoutSeconds,
	}
	param.KafkaMeta = meta

	stream := &kafkaStreamReader{
		cl:        cl,
		meta:      meta,
		topic:     ks.Topic,
		partition: ks.Partition,
		timeout:   time.Duration(ks.TimeoutSeconds) * time.Second,
		sizeCap:   ks.Size,
		ctx:       proc.Ctx,
	}

	parser, err := newCSVParserFromReader(param.Extern, stream)
	if err != nil {
		cl.Close()
		return false, err
	}
	r.cl = cl
	r.stream = stream
	r.ks = ks
	r.proc = proc
	r.csv.param = param
	r.csv.reader = stream
	r.csv.plh = &ParseLineHandler{csvReader: parser}
	return false, nil
}

func (r *KafkaReader) ReadBatch(ctx context.Context, bat *batch.Batch, proc *process.Process, analyzer process.Analyzer) (fileFinished bool, err error) {
	fileFinished, err = r.csv.makeBatchRows(proc, bat)
	if err == nil && fileFinished {
		r.completed = true
	}
	return fileFinished, err
}

func (r *KafkaReader) Close() error {
	if r.cl == nil {
		return nil
	}
	// A clean, complete read commits progress (autocommit=true) and records
	// the last message id for LAST_KAFKA_MESSAGE_ID(). An aborted read (error
	// or cancel) commits nothing and leaves the session id untouched.
	if r.completed && r.stream != nil && r.stream.anyRead {
		if r.ks.Autocommit {
			// committed offset is the NEXT offset to read (Kafka convention)
			_ = kafkaCommit(context.Background(), r.cl, r.ks, r.stream.last+1)
		}
		if r.proc != nil {
			if ses, ok := r.proc.GetSession().(process.KafkaSessionState); ok {
				ses.SetLastKafkaMessageID(r.stream.last)
			}
		}
	}
	r.cl.Close()
	r.cl = nil
	r.stream = nil
	if r.csv.param != nil {
		r.csv.param.KafkaMeta = nil
	}
	r.csv.plh = nil
	r.csv.reader = nil
	return nil
}

// kafkaCommit commits `at` as the consumer group's offset (the next offset to
// be read) for the scan's partition.
func kafkaCommit(ctx context.Context, cl *kgo.Client, ks *plan.KafkaScan, at int64) error {
	adm := kadm.NewClient(cl)
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: ks.Topic, Partition: ks.Partition, At: at, LeaderEpoch: -1})
	if _, err := adm.CommitOffsets(ctx, ks.Group, offsets); err != nil {
		return moerr.NewInternalErrorf(ctx, "kafka: commit offset %d for group %s failed: %v", at, ks.Group, err)
	}
	return nil
}
