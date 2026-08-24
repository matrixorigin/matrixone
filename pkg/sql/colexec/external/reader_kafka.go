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
	"github.com/matrixorigin/matrixone/pkg/logutil"
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

// KafkaMetaState carries the CURRENT message's metadata for the hidden
// __mo_message_* columns plus the effective read controls. Each message is
// parsed independently (KafkaReader.ReadBatch), so record↔message pairing is
// exact by construction — malformed messages can never compensate for each
// other across boundaries.
type KafkaMetaState struct {
	cur KafkaMsgMeta
	loc *time.Location
	// effective read controls, surfaced through the __mo_read_* columns
	startID int64
	size    int64
	timeout int64
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

// kafkaMessageSource yields Kafka messages one at a time. The read ends at
// the size cap, or when no new message arrives within the idle timeout; a
// deadline/cancel on the QUERY context always aborts (it is never a clean
// end). Everything runs on the scan goroutine.
type kafkaMessageSource struct {
	cl      *kgo.Client
	buf     []KafkaMsgMeta // fetched, not yet consumed
	done    bool
	timeout time.Duration // 0 = block forever
	sizeCap int64         // 0 = unlimited
	readCnt int64
	last    int64 // offset of the last message returned
	anyRead bool
	ctx     context.Context
}

// next returns the next message; ok=false with a nil error is the clean end
// of the read (size cap reached or idle timeout expired).
func (s *kafkaMessageSource) next() (KafkaMsgMeta, bool, error) {
	for {
		// the size cap applies to messages SERVED, including ones already
		// buffered from an earlier poll
		if s.sizeCap > 0 && s.readCnt >= s.sizeCap {
			s.done = true
			return KafkaMsgMeta{}, false, nil
		}
		if len(s.buf) > 0 {
			msg := s.buf[0]
			s.buf = s.buf[1:]
			s.readCnt++
			s.last = msg.Offset
			s.anyRead = true
			return msg, true, nil
		}
		if s.done {
			return KafkaMsgMeta{}, false, nil
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
				// only OUR per-poll deadline is a clean idle-timeout end; a
				// deadline on the query context itself (max_execution_time)
				// must abort the scan, or a timed-out query would commit and
				// record messages the client never received
				if s.ctx.Err() == nil {
					timedOut = true
				} else if pollErr == nil {
					pollErr = s.ctx.Err()
				}
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
			return KafkaMsgMeta{}, false, pollErr
		}
		got := false
		fetches.EachRecord(func(rec *kgo.Record) {
			got = true
			key := ""
			nilKey := rec.Key == nil
			if !nilKey {
				key = string(rec.Key)
			}
			s.buf = append(s.buf, KafkaMsgMeta{
				Offset: rec.Offset,
				Ts:     rec.Timestamp,
				Key:    key,
				NilKey: nilKey,
				Value:  string(rec.Value),
			})
		})
		if !got {
			if timedOut {
				s.done = true
				return KafkaMsgMeta{}, false, nil
			}
			if err := s.ctx.Err(); err != nil {
				return KafkaMsgMeta{}, false, err
			}
			// blocking mode (timeout 0) with an empty poll: poll again
		}
	}
}

// KafkaReader is the ExternalFileReader of a Kafka external table: it reads
// one topic partition from a resolved start offset, parses each message value
// as one csv/jsonl record with the shared CSV machinery, and surfaces message
// metadata through the hidden __mo_message_* columns.
type KafkaReader struct {
	csv    CsvReader
	source *kafkaMessageSource
	cl     *kgo.Client
	ks     *plan.KafkaScan
	proc   *process.Process
	// completed is set only when ReadBatch drained the source without error;
	// the pending-progress handoff in Close depends on it (an aborted read
	// must change nothing).
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

	// Fetch the partition bounds first. This fails FAST and LOUDLY on an
	// unreachable broker, a missing topic, or a nonexistent partition — the
	// alternative is a poll that quietly times out and returns an empty
	// result indistinguishable from "no new messages". The bounds also
	// validate an explicit start id (below the log start or beyond the end
	// is an error, never a silent reset) and resolve the -1/earliest
	// boundary without a second admin round-trip.
	seedCl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(ks.Brokers, ",")...))
	if err != nil {
		return false, moerr.NewInternalErrorf(proc.Ctx, "kafka: cannot create client for %s: %v", ks.Brokers, err)
	}
	logStart, logEnd, err := kafkaPartitionBounds(proc.Ctx, seedCl, ks)
	seedCl.Close()
	if err != nil {
		return false, err
	}

	// Resolve the FIRST offset to read. start_id is "the last consumed
	// offset": reading begins after it. -1 means earliest (inclusive) with
	// autocommit=false, latest with autocommit=true; 0 with autocommit=true
	// keeps the issue-specified "0 = earliest (inclusive)" meaning.
	var readFrom int64
	switch {
	case ks.HasStartId && ks.StartId == -1 && ks.Autocommit:
		readFrom = logEnd
	case ks.HasStartId && ks.StartId == -1 && !ks.Autocommit:
		readFrom = logStart
	case ks.HasStartId && ks.StartId == 0 && ks.Autocommit:
		readFrom = logStart
	case ks.HasStartId:
		readFrom = ks.StartId + 1
		if readFrom < logStart {
			return false, moerr.NewInvalidInputf(proc.Ctx,
				"kafka: __mo_read_start_id %d is below the partition log start %d (messages expired?); use -1 to read from the earliest",
				ks.StartId, logStart)
		}
		if readFrom > logEnd {
			return false, moerr.NewInvalidInputf(proc.Ctx,
				"kafka: __mo_read_start_id %d is beyond the partition end (last offset %d)",
				ks.StartId, logEnd-1)
		}
	default:
		// only reachable with autocommit=true (compile enforces the
		// requirement for autocommit=false): default 0 = earliest
		readFrom = logStart
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(ks.Brokers, ",")...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			ks.Topic: {ks.Partition: kgo.NewOffset().At(readFrom)},
		}),
		// A validated exact offset can only fall out of range if retention
		// trims the log mid-scan; resetting to the END means such a race can
		// skip expired messages but can never silently REPLAY delivered ones
		// (kgo's default reset is AtStart, which would).
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		// SQL rows are Kafka's COMMITTED view: records from aborted (or
		// still-open) producer transactions must never surface as rows,
		// become the last message id, or advance committed progress. kgo's
		// default is read_uncommitted.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		return false, moerr.NewInternalErrorf(proc.Ctx, "kafka: cannot create client for %s: %v", ks.Brokers, err)
	}

	// With autocommit=false the read position is committed BEFORE reading
	// (issue #27518): committing the same start twice is idempotent, so a
	// retried read with the same __mo_read_start_id returns the same data.
	if !ks.Autocommit {
		if err := kafkaCommit(proc.Ctx, cl, ks, readFrom); err != nil {
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

	r.source = &kafkaMessageSource{
		cl:      cl,
		timeout: time.Duration(ks.TimeoutSeconds) * time.Second,
		sizeCap: ks.Size,
		ctx:     proc.Ctx,
	}
	r.cl = cl
	r.ks = ks
	r.proc = proc
	r.csv.param = param
	return false, nil
}

// parseOneMessage parses ONE message value into exactly one record of the
// table's format. Each message is parsed independently, so a malformed
// message fails on its own boundary with its own offset — messages can never
// compensate for each other.
func (r *KafkaReader) parseOneMessage(ctx context.Context, param *ExternalParam, msg *KafkaMsgMeta) ([]csvparser.Field, error) {
	value := strings.TrimRight(msg.Value, "\r\n")
	if param.Extern.Format == tree.JSONLINE {
		fields, err := r.csv.transJson2Lines(ctx, value, param.Attrs, param.Cols, param.Extern.JsonData)
		if err != nil {
			// no streaming-LOAD resume across message boundaries
			r.csv.prevStr = ""
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil, moerr.NewInvalidInputf(ctx,
					"kafka message at offset %d is not a complete JSON object", msg.Offset)
			}
			return nil, moerr.NewInvalidInputf(ctx,
				"kafka message at offset %d is not exactly one JSON object: %v", msg.Offset, err)
		}
		return fields, nil
	}
	parser, err := newCSVParserFromReader(param.Extern, strings.NewReader(value+"\n"))
	if err != nil {
		return nil, err
	}
	row, err := parser.Read(nil)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, moerr.NewInvalidInputf(ctx,
				"kafka message at offset %d did not parse to exactly one record (empty value)", msg.Offset)
		}
		return nil, err
	}
	if _, err := parser.Read(nil); !errors.Is(err, io.EOF) {
		return nil, moerr.NewInvalidInputf(ctx,
			"kafka message at offset %d did not parse to exactly one record", msg.Offset)
	}
	return row, nil
}

func (r *KafkaReader) ReadBatch(ctx context.Context, bat *batch.Batch, proc *process.Process, analyzer process.Analyzer) (fileFinished bool, err error) {
	if bat == nil || bat.VectorCount() == 0 {
		return false, moerr.NewInternalError(proc.Ctx, "kafka reader requires at least one materialized column")
	}
	param := r.csv.param
	var curBatchSize uint64
	for i := 0; i < OneBatchMaxRow; i++ {
		// a cancelled/timed-out QUERY aborts; it is never a clean end
		if err := proc.Ctx.Err(); err != nil {
			return false, err
		}
		msg, ok, err := r.source.next()
		if err != nil {
			return false, err
		}
		if !ok {
			fileFinished = true
			r.completed = true
			break
		}
		row, err := r.parseOneMessage(proc.Ctx, param, &msg)
		if err != nil {
			return false, err
		}
		param.KafkaMeta.cur = msg
		if err := getOneRowData(proc, bat, row, i, param); err != nil {
			return false, err
		}
		curBatchSize += uint64(len(msg.Value))
		if curBatchSize >= param.maxBatchSize {
			break
		}
	}
	bat.SetRowCount(bat.Vecs[0].Length())
	return fileFinished, nil
}

func (r *KafkaReader) Close() error {
	if r.cl == nil {
		return nil
	}
	// A clean, complete read HANDS OFF its progress side effects (autocommit
	// offset + LAST_KAFKA_MESSAGE_ID) as pending state: Close runs when the
	// SOURCE drains, but the final batch has not yet survived downstream
	// operators or delivery. External.Reset publishes the pending progress
	// only when the whole statement succeeded and discards it otherwise —
	// an aborted statement must change nothing, no matter where it aborts.
	// An aborted read (error/cancel before EOF) just closes the transport.
	if r.completed && r.source != nil && r.source.anyRead && r.csv.param != nil {
		r.csv.param.KafkaPending = &KafkaPendingProgress{
			cl:     r.cl,
			ks:     r.ks,
			lastID: r.source.last,
		}
		r.cl = nil // ownership moved to the pending progress
	}
	if r.cl != nil {
		r.cl.Close()
		r.cl = nil
	}
	r.source = nil
	if r.csv.param != nil {
		r.csv.param.KafkaMeta = nil
	}
	r.csv.plh = nil
	r.csv.reader = nil
	return nil
}

// KafkaPendingProgress is the deferred side-effect state of a drained Kafka
// scan: the committed-offset advance and the session's last message id. It
// owns the client until Finalize.
type KafkaPendingProgress struct {
	cl     *kgo.Client
	ks     *plan.KafkaScan
	lastID int64
}

// Finalize publishes the progress when the statement SUCCEEDED (commit
// last+1 for autocommit=true, record the last id for
// LAST_KAFKA_MESSAGE_ID()) and discards it otherwise, then releases the
// client. Idempotent via the caller nil-ing the pending pointer.
func (p *KafkaPendingProgress) Finalize(proc *process.Process, success bool) {
	if p == nil || p.cl == nil {
		return
	}
	if success {
		if p.ks.Autocommit {
			// committed offset is the NEXT offset to read (Kafka convention).
			// Bounded: this runs synchronously in statement teardown and must
			// not stall behind kgo's retry budget on a dead broker; a failed
			// progress commit is logged (the read itself succeeded, and
			// ON-mode reads never depend on committed progress).
			cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			if err := kafkaCommit(cctx, p.cl, p.ks, p.lastID+1); err != nil {
				logutil.Warnf("kafka: autocommit of offset %d for group %s failed: %v",
					p.lastID+1, p.ks.Group, err)
			}
			cancel()
		}
		if proc != nil {
			if ses, ok := proc.GetSession().(process.KafkaSessionState); ok {
				ses.SetLastKafkaMessageID(p.lastID)
			}
		}
	}
	p.cl.Close()
	p.cl = nil
}

// kafkaPartitionBounds returns the partition's [logStart, logEnd) offsets.
// logEnd is the high watermark: the offset the NEXT produced message gets.
func kafkaPartitionBounds(ctx context.Context, cl *kgo.Client, ks *plan.KafkaScan) (int64, int64, error) {
	adm := kadm.NewClient(cl)
	starts, err := adm.ListStartOffsets(ctx, ks.Topic)
	if err != nil {
		return 0, 0, moerr.NewInternalErrorf(ctx, "kafka: cannot reach %s or list offsets of topic %q: %v", ks.Brokers, ks.Topic, err)
	}
	lo, ok := starts.Lookup(ks.Topic, ks.Partition)
	if !ok || lo.Err != nil {
		return 0, 0, moerr.NewInvalidInputf(ctx, "kafka: topic %q has no partition %d", ks.Topic, ks.Partition)
	}
	ends, err := adm.ListEndOffsets(ctx, ks.Topic)
	if err != nil {
		return 0, 0, moerr.NewInternalErrorf(ctx, "kafka: cannot list end offsets of topic %q: %v", ks.Topic, err)
	}
	hi, ok := ends.Lookup(ks.Topic, ks.Partition)
	if !ok || hi.Err != nil {
		return 0, 0, moerr.NewInvalidInputf(ctx, "kafka: topic %q has no partition %d", ks.Topic, ks.Partition)
	}
	return lo.Offset, hi.Offset, nil
}

// kafkaCommit commits `at` as the consumer group's offset (the next offset to
// be read) for the scan's partition.
func kafkaCommit(ctx context.Context, cl *kgo.Client, ks *plan.KafkaScan, at int64) error {
	adm := kadm.NewClient(cl)
	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: ks.Topic, Partition: ks.Partition, At: at, LeaderEpoch: -1})
	resp, err := adm.CommitOffsets(ctx, ks.Group, offsets)
	if err != nil {
		return moerr.NewInternalErrorf(ctx, "kafka: commit offset %d for group %s failed: %v", at, ks.Group, err)
	}
	// kadm reports authorization and partition-level failures inside the
	// response while the request-level error stays nil
	if perr := resp.Error(); perr != nil {
		return moerr.NewInternalErrorf(ctx, "kafka: commit offset %d for group %s failed: %v", at, ks.Group, perr)
	}
	return nil
}
