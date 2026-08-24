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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	sqlkafka "github.com/matrixorigin/matrixone/pkg/sql/kafka"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// fakeKafkaSession adds process.KafkaSessionState on top of fakeScanSession.
type fakeKafkaSession struct {
	fakeScanSession
	lastID  int64
	lastSet bool
}

func (s *fakeKafkaSession) SetLastKafkaMessageID(id int64) { s.lastID, s.lastSet = id, true }
func (s *fakeKafkaSession) LastKafkaMessageID() (int64, bool) {
	return s.lastID, s.lastSet
}

// startKafka spins an in-process fake Kafka cluster with one seeded topic and
// produces msgs (key\x00value pairs; empty key means nil key) into partition 0.
func startKafka(t *testing.T, topic string, msgs [][2]string) string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	require.NoError(t, err)
	t.Cleanup(c.Close)
	addr := c.ListenAddrs()[0]
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for _, kv := range msgs {
		rec := &kgo.Record{Topic: topic, Value: []byte(kv[1])}
		if kv[0] != "" {
			rec.Key = []byte(kv[0])
		}
		require.NoError(t, cl.ProduceSync(ctx, rec).FirstErr())
	}
	return addr
}

func kafkaScan(addr, topic, format string) *plan.KafkaScan {
	return &plan.KafkaScan{
		Brokers:        addr,
		Topic:          topic,
		Partition:      0,
		Autocommit:     true,
		Group:          "g_test",
		Format:         format,
		Separator:      ",",
		HasStartId:     true,
		StartId:        0, // autocommit: 0 = earliest inclusive
		Size:           0,
		TimeoutSeconds: 2,
	}
}

// newKafkaTestParam declares (a int, s varchar) plus every synthetic column.
func newKafkaTestParam(t *testing.T, ks *plan.KafkaScan) (*ExternalParam, *process.Process, *batch.Batch) {
	t.Helper()
	proc := testutil.NewProc(t)
	proc.Session = &fakeKafkaSession{}
	cols := []*plan.ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: "s", Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.KafkaMessageID, ColId: catalog.KafkaMessageIDColId, Typ: plan.Type{Id: int32(types.T_int64)}},
		{Name: catalog.KafkaMessageTS, ColId: catalog.KafkaMessageTSColId, Typ: plan.Type{Id: int32(types.T_timestamp), Scale: 3}},
		{Name: catalog.KafkaMessageKey, ColId: catalog.KafkaMessageKeyColId, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.KafkaMessageValue, ColId: catalog.KafkaMessageValueColId, Typ: plan.Type{Id: int32(types.T_varchar), Width: types.MaxVarcharLen}},
		{Name: catalog.KafkaReadStartID, ColId: catalog.KafkaReadStartIDColId, Typ: plan.Type{Id: int32(types.T_int64)}},
	}
	attrs := make([]plan.ExternAttr, len(cols))
	names := make([]string, len(cols))
	for i, c := range cols {
		fieldIdx := int32(i)
		if i >= 2 {
			fieldIdx = 0 // synthetic: never read from the line
		}
		attrs[i] = plan.ExternAttr{ColName: c.Name, ColIndex: int32(i), ColFieldIndex: fieldIdx}
		names[i] = c.Name
	}
	param := &ExternalParam{
		ExParamConst: ExParamConst{
			Attrs:         attrs,
			Cols:          cols,
			ColumnListLen: 2, // data columns in each message record
			Extern:        KafkaExternParam(ks),
			KafkaScan:     ks,
			StrictSqlMode: true,
			maxBatchSize:  1 << 20,
		},
		ExParam: ExParam{
			Fileparam: &ExFileparam{FileCnt: 1},
			Filter:    &FilterParam{},
		},
	}
	bat := batch.NewOffHeap(names)
	for i := range cols {
		bat.Vecs[i] = vector.NewOffHeapVecWithType(makeType(&cols[i].Typ, false))
	}
	t.Cleanup(func() { bat.Clean(proc.Mp()) })
	return param, proc, bat
}

func readAllKafka(t *testing.T, param *ExternalParam, proc *process.Process, bat *batch.Batch) *KafkaReader {
	t.Helper()
	r := NewKafkaReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	for {
		finished, err := r.ReadBatch(proc.Ctx, bat, proc, nil)
		require.NoError(t, err)
		if finished {
			break
		}
	}
	require.NoError(t, r.Close())
	return r
}

func fetchCommitted(t *testing.T, addr, group, topic string) (int64, bool) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	require.NoError(t, err)
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	resp, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		// an uncommitted group does not exist at all
		require.Contains(t, err.Error(), "GROUP_ID_NOT_FOUND")
		return 0, false
	}
	o, ok := resp.Lookup(topic, 0)
	if !ok {
		return 0, false
	}
	return o.At, o.At >= 0
}

// TestKafkaReaderCSV reads five CSV messages from the earliest offset:
// declared columns are parsed, the synthetic columns carry per-message
// metadata, the completed autocommit scan commits last+1, and the session
// records the last message id for LAST_KAFKA_MESSAGE_ID().
func TestKafkaReaderCSV(t *testing.T) {
	addr := startKafka(t, "t_csv", [][2]string{
		{"k0", "1,alpha"}, {"", "2,beta"}, {"k2", "3,gamma"}, {"k3", "4,delta"}, {"k4", "5,"},
	})
	ks := kafkaScan(addr, "t_csv", sqlkafka.FormatCSV)
	param, proc, bat := newKafkaTestParam(t, ks)

	readAllKafka(t, param, proc, bat)
	require.Equal(t, 5, bat.RowCount())

	as := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
	require.Equal(t, []int32{1, 2, 3, 4, 5}, as[:5])
	require.Equal(t, "alpha", bat.Vecs[1].GetStringAt(0))
	ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[2])
	require.Equal(t, []int64{0, 1, 2, 3, 4}, ids[:5])
	// nil key -> NULL
	require.True(t, bat.Vecs[4].GetNulls().Contains(1))
	require.Equal(t, "k0", bat.Vecs[4].GetStringAt(0))
	// raw message value
	require.Equal(t, "3,gamma", bat.Vecs[5].GetStringAt(2))
	// effective start id control column
	starts := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[6])
	require.Equal(t, int64(0), starts[0])
	// timestamps parse and are recent
	tss := vector.MustFixedColWithTypeCheck[types.Timestamp](bat.Vecs[3])
	require.NotZero(t, tss[0])

	// completed autocommit scan: committed offset = last+1, session id = last
	at, ok := fetchCommitted(t, addr, "g_test", "t_csv")
	require.True(t, ok)
	require.Equal(t, int64(5), at)
	ses := proc.GetSession().(*fakeKafkaSession)
	require.True(t, ses.lastSet)
	require.Equal(t, int64(4), ses.lastID)
}

// TestKafkaReaderStartAfter proves __mo_read_start_id positions the read
// after the given offset, and autocommit=false commits that position at Open
// (idempotent re-read) without committing progress afterwards.
func TestKafkaReaderStartAfter(t *testing.T) {
	addr := startKafka(t, "t_pos", [][2]string{
		{"", "1,a"}, {"", "2,b"}, {"", "3,c"}, {"", "4,d"},
	})
	ks := kafkaScan(addr, "t_pos", sqlkafka.FormatCSV)
	ks.Autocommit = false
	ks.StartId = 1 // last consumed: read 2..3 (offsets 2,3)
	param, proc, bat := newKafkaTestParam(t, ks)

	readAllKafka(t, param, proc, bat)
	require.Equal(t, 2, bat.RowCount())
	ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[2])
	require.Equal(t, []int64{2, 3}, ids[:2])

	// committed at Open: start+1, NOT advanced by the read
	at, ok := fetchCommitted(t, addr, "g_test", "t_pos")
	require.True(t, ok)
	require.Equal(t, int64(2), at)
	// the session still records the last id for chaining
	require.Equal(t, int64(3), proc.GetSession().(*fakeKafkaSession).lastID)
}

// TestKafkaReaderSizeCap stops after __mo_read_size messages.
func TestKafkaReaderSizeCap(t *testing.T) {
	addr := startKafka(t, "t_cap", [][2]string{
		{"", "1,a"}, {"", "2,b"}, {"", "3,c"},
	})
	ks := kafkaScan(addr, "t_cap", sqlkafka.FormatCSV)
	ks.Size = 2
	param, proc, bat := newKafkaTestParam(t, ks)
	readAllKafka(t, param, proc, bat)
	require.Equal(t, 2, bat.RowCount())
	require.Equal(t, int64(1), proc.GetSession().(*fakeKafkaSession).lastID)
}

// TestKafkaReaderJSONL parses each message value as one JSON object and still
// synthesizes the metadata columns.
func TestKafkaReaderJSONL(t *testing.T) {
	addr := startKafka(t, "t_json", [][2]string{
		{"", `{"a": 7, "s": "x"}`}, {"", `{"s": "y", "a": 8}`},
	})
	ks := kafkaScan(addr, "t_json", sqlkafka.FormatJSONL)
	param, proc, bat := newKafkaTestParam(t, ks)
	readAllKafka(t, param, proc, bat)
	require.Equal(t, 2, bat.RowCount())
	as := vector.MustFixedColWithTypeCheck[int32](bat.Vecs[0])
	require.Equal(t, []int32{7, 8}, as[:2])
	require.Equal(t, "y", bat.Vecs[1].GetStringAt(1))
	ids := vector.MustFixedColWithTypeCheck[int64](bat.Vecs[2])
	require.Equal(t, []int64{0, 1}, ids[:2])
	require.Equal(t, `{"s": "y", "a": 8}`, bat.Vecs[5].GetStringAt(1))
}

// TestKafkaReaderErrors covers the fail-closed record contract: a message
// with the wrong field count and a message that parses into two records are
// both errors, and an aborted scan neither commits nor updates the session.
func TestKafkaReaderErrors(t *testing.T) {
	// wrong field count under strict mode
	addr := startKafka(t, "t_bad", [][2]string{{"", "1,a"}, {"", "1,a,extra"}})
	ks := kafkaScan(addr, "t_bad", sqlkafka.FormatCSV)
	param, proc, bat := newKafkaTestParam(t, ks)
	r := NewKafkaReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	readErr := func() error {
		for {
			finished, err := r.ReadBatch(proc.Ctx, bat, proc, nil)
			if err != nil || finished {
				return err
			}
		}
	}
	require.ErrorContains(t, readErr(), "not equal to input columns")
	require.NoError(t, r.Close())
	require.False(t, proc.GetSession().(*fakeKafkaSession).lastSet,
		"aborted scan must not update the session last id")
	_, ok := fetchCommitted(t, addr, "g_test", "t_bad")
	require.False(t, ok, "aborted autocommit scan must not commit")

	// a message value containing a record separator parses into two records
	addr2 := startKafka(t, "t_two", [][2]string{{"", "1,a\n2,b"}})
	ks2 := kafkaScan(addr2, "t_two", sqlkafka.FormatCSV)
	param2, proc2, bat2 := newKafkaTestParam(t, ks2)
	r2 := NewKafkaReader(param2)
	_, err = r2.Open(param2, proc2)
	require.NoError(t, err)
	for {
		finished, rerr := r2.ReadBatch(proc2.Ctx, bat2, proc2, nil)
		if rerr != nil {
			require.ErrorContains(t, rerr, "exactly one record")
			break
		}
		require.False(t, finished, "the mismatch must surface as an error")
	}
	require.NoError(t, r2.Close())
}

// TestKafkaExternParam pins the synthetic ExternParam shapes.
func TestKafkaExternParam(t *testing.T) {
	csv := KafkaExternParam(&plan.KafkaScan{Format: sqlkafka.FormatCSV, Separator: "|"})
	require.Equal(t, "|", csv.Tail.Fields.Terminated.Value)
	require.Equal(t, byte(0), csv.Tail.Fields.EscapedBy.Value)
	jl := KafkaExternParam(&plan.KafkaScan{Format: sqlkafka.FormatJSONL})
	require.Equal(t, "jsonline", strings.ToLower(string(jl.Format)))
	require.NotEmpty(t, jl.JsonData)
}

// TestKafkaReaderStartBounds: an explicit __mo_read_start_id outside the
// partition's real bounds must FAIL, never silently reset (kgo's default
// out-of-range reset is AtStart, which would replay the whole topic).
func TestKafkaReaderStartBounds(t *testing.T) {
	addr := startKafka(t, "t_bounds", [][2]string{{"", "1,a"}, {"", "2,b"}, {"", "3,c"}})

	// beyond the end (offsets 0..2 exist, end=3): start=50 is an error
	ks := kafkaScan(addr, "t_bounds", sqlkafka.FormatCSV)
	ks.Autocommit = false
	ks.StartId = 50
	param, proc, _ := newKafkaTestParam(t, ks)
	_, err := NewKafkaReader(param).Open(param, proc)
	require.ErrorContains(t, err, "beyond the partition end")
	require.ErrorContains(t, err, "last offset 2")

	// start exactly at the end (last consumed = last existing) is VALID: it
	// waits for new messages (the zero-message case)
	ks2 := kafkaScan(addr, "t_bounds", sqlkafka.FormatCSV)
	ks2.Autocommit = false
	ks2.StartId = 2
	ks2.TimeoutSeconds = 1
	param2, proc2, bat2 := newKafkaTestParam(t, ks2)
	readAllKafka(t, param2, proc2, bat2)
	require.Equal(t, 0, bat2.RowCount())

	// advance the log start past offset 1 via DeleteRecords, then a start id
	// below the new log start is an error
	{
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
		require.NoError(t, err)
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		offsets := make(kadm.Offsets)
		offsets.Add(kadm.Offset{Topic: "t_bounds", Partition: 0, At: 2, LeaderEpoch: -1})
		_, err = kadm.NewClient(cl).DeleteRecords(ctx, offsets)
		require.NoError(t, err)
	}
	ks3 := kafkaScan(addr, "t_bounds", sqlkafka.FormatCSV)
	ks3.Autocommit = false
	ks3.StartId = 0 // read-from 1 < new log start 2
	param3, proc3, _ := newKafkaTestParam(t, ks3)
	_, err = NewKafkaReader(param3).Open(param3, proc3)
	require.ErrorContains(t, err, "below the partition log start")
}

// TestKafkaReaderConnectErrors: an unreachable broker or a missing
// topic/partition fails LOUDLY at Open instead of returning an empty result
// after a silent poll timeout.
func TestKafkaReaderConnectErrors(t *testing.T) {
	ks := kafkaScan("127.0.0.1:1", "nope", sqlkafka.FormatCSV)
	param, proc, _ := newKafkaTestParam(t, ks)
	_, err := NewKafkaReader(param).Open(param, proc)
	require.Error(t, err)
	require.Contains(t, err.Error(), "kafka:")

	// live broker, wrong partition
	addr := startKafka(t, "t_conn", [][2]string{{"", "1,a"}})
	ks2 := kafkaScan(addr, "t_conn", sqlkafka.FormatCSV)
	ks2.Partition = 7
	param2, proc2, _ := newKafkaTestParam(t, ks2)
	_, err = NewKafkaReader(param2).Open(param2, proc2)
	require.ErrorContains(t, err, "no partition 7")
}

// TestKafkaReaderQueryDeadlineAborts: a deadline on the QUERY context must
// abort a blocking read (timeout=0) as an error — never masquerade as a
// clean idle-timeout that commits offsets and records a last id for rows the
// client never received.
func TestKafkaReaderQueryDeadlineAborts(t *testing.T) {
	addr := startKafka(t, "t_ddl", [][2]string{{"", "1,a"}})
	ks := kafkaScan(addr, "t_ddl", sqlkafka.FormatCSV)
	ks.TimeoutSeconds = 0 // block forever: only the query deadline can end it
	param, proc, bat := newKafkaTestParam(t, ks)
	ctx, cancel := context.WithTimeout(proc.Ctx, 1500*time.Millisecond)
	defer cancel()
	proc.Ctx = ctx

	r := NewKafkaReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	var readErr error
	for {
		var finished bool
		finished, readErr = r.ReadBatch(proc.Ctx, bat, proc, nil)
		if readErr != nil || finished {
			break
		}
	}
	require.Error(t, readErr, "query deadline must surface as an error, not clean EOF")
	require.NoError(t, r.Close())
	require.False(t, proc.GetSession().(*fakeKafkaSession).lastSet,
		"deadline-aborted scan must not record a last id")
	_, ok := fetchCommitted(t, addr, "g_test", "t_ddl")
	require.False(t, ok, "deadline-aborted autocommit scan must not commit")
}

// TestKafkaReaderCancelAtBatchBoundary: a KILL landing exactly between
// batches (ctx already cancelled when makeBatchRows starts) must abort the
// scan, not complete it.
func TestKafkaReaderCancelAtBatchBoundary(t *testing.T) {
	addr := startKafka(t, "t_kill", [][2]string{{"", "1,a"}, {"", "2,b"}})
	ks := kafkaScan(addr, "t_kill", sqlkafka.FormatCSV)
	param, proc, bat := newKafkaTestParam(t, ks)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx

	r := NewKafkaReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	cancel() // the KILL lands before the next batch starts
	_, err = r.ReadBatch(proc.Ctx, bat, proc, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, r.Close())
	require.False(t, proc.GetSession().(*fakeKafkaSession).lastSet)
	_, ok := fetchCommitted(t, addr, "g_test", "t_kill")
	require.False(t, ok, "killed autocommit scan must not commit")
}

// TestKafkaReaderTruncatedJSONL: a message that is not a complete JSON
// object is a hard error — the streaming-LOAD resume path must not merge the
// fragment with the next message or silently drop rows and commit past them.
func TestKafkaReaderTruncatedJSONL(t *testing.T) {
	addr := startKafka(t, "t_trunc", [][2]string{
		{"", `{"a": 1, "s": "x"}`}, {"", `{"a": 2`},
	})
	ks := kafkaScan(addr, "t_trunc", sqlkafka.FormatJSONL)
	param, proc, bat := newKafkaTestParam(t, ks)
	r := NewKafkaReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	for {
		var finished bool
		finished, err = r.ReadBatch(proc.Ctx, bat, proc, nil)
		if err != nil || finished {
			break
		}
	}
	require.ErrorContains(t, err, "not a complete JSON object")
	require.NoError(t, r.Close())
	require.False(t, proc.GetSession().(*fakeKafkaSession).lastSet)
	_, ok := fetchCommitted(t, addr, "g_test", "t_trunc")
	require.False(t, ok)
}

// TestKafkaReaderEmptyMessageValue: an empty message value parses to zero
// CSV records and must fail the message-record pairing, not silently skip.
func TestKafkaReaderEmptyMessageValue(t *testing.T) {
	addr := startKafka(t, "t_empty", [][2]string{{"", "1,a"}, {"", ""}})
	ks := kafkaScan(addr, "t_empty", sqlkafka.FormatCSV)
	param, proc, bat := newKafkaTestParam(t, ks)
	r := NewKafkaReader(param)
	_, err := r.Open(param, proc)
	require.NoError(t, err)
	for {
		var finished bool
		finished, err = r.ReadBatch(proc.Ctx, bat, proc, nil)
		if err != nil || finished {
			break
		}
	}
	require.ErrorContains(t, err, "exactly one")
	require.NoError(t, r.Close())
}

// TestKafkaReaderOffsetMatrix covers the start-position cells the other
// tests miss: autocommit=false with -1 (earliest + commit log start),
// autocommit=true with -1 (latest: nothing yet) and with an explicit n.
func TestKafkaReaderOffsetMatrix(t *testing.T) {
	addr := startKafka(t, "t_matrix", [][2]string{
		{"", "1,a"}, {"", "2,b"}, {"", "3,c"}, {"", "4,d"}, {"", "5,e"},
	})

	// OFF, start=-1: read everything from the earliest, commit the log start
	ks := kafkaScan(addr, "t_matrix", sqlkafka.FormatCSV)
	ks.Autocommit = false
	ks.StartId = -1
	param, proc, bat := newKafkaTestParam(t, ks)
	readAllKafka(t, param, proc, bat)
	require.Equal(t, 5, bat.RowCount())
	at, ok := fetchCommitted(t, addr, "g_test", "t_matrix")
	require.True(t, ok)
	require.Equal(t, int64(0), at, "OFF/-1 commits the log start")
	require.Equal(t, int64(4), proc.GetSession().(*fakeKafkaSession).lastID)

	// ON, start=-1 (latest): nothing new -> 0 rows, no session id, no commit
	ks2 := kafkaScan(addr, "t_matrix", sqlkafka.FormatCSV)
	ks2.StartId = -1
	ks2.Group = "g_latest"
	ks2.TimeoutSeconds = 1
	param2, proc2, bat2 := newKafkaTestParam(t, ks2)
	readAllKafka(t, param2, proc2, bat2)
	require.Equal(t, 0, bat2.RowCount())
	require.False(t, proc2.GetSession().(*fakeKafkaSession).lastSet)
	_, ok = fetchCommitted(t, addr, "g_latest", "t_matrix")
	require.False(t, ok, "an empty completed read commits nothing")

	// ON, explicit start=2: reads offsets 3..4 and commits 5
	ks3 := kafkaScan(addr, "t_matrix", sqlkafka.FormatCSV)
	ks3.StartId = 2
	ks3.Group = "g_on_n"
	param3, proc3, bat3 := newKafkaTestParam(t, ks3)
	readAllKafka(t, param3, proc3, bat3)
	require.Equal(t, 2, bat3.RowCount())
	ids := vector.MustFixedColWithTypeCheck[int64](bat3.Vecs[2])
	require.Equal(t, []int64{3, 4}, ids[:2])
	at, ok = fetchCommitted(t, addr, "g_on_n", "t_matrix")
	require.True(t, ok)
	require.Equal(t, int64(5), at)
}

// TestKafkaReaderTimestampInstant: the same produced instant yields the SAME
// internal timestamp value in two different session zones — the rendered
// wall clock differs, the instant does not.
func TestKafkaReaderTimestampInstant(t *testing.T) {
	fixed := time.Date(2026, 3, 15, 10, 30, 0, 123_000_000, time.UTC)
	produce := func(topic string) string {
		c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
		require.NoError(t, err)
		t.Cleanup(c.Close)
		addr := c.ListenAddrs()[0]
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
		require.NoError(t, err)
		defer cl.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		require.NoError(t, cl.ProduceSync(ctx,
			&kgo.Record{Topic: topic, Value: []byte("1,a"), Timestamp: fixed}).FirstErr())
		return addr
	}

	readTS := func(topic, addr string, loc *time.Location) types.Timestamp {
		ks := kafkaScan(addr, topic, sqlkafka.FormatCSV)
		param, proc, bat := newKafkaTestParam(t, ks)
		proc.GetSessionInfo().TimeZone = loc
		readAllKafka(t, param, proc, bat)
		require.Equal(t, 1, bat.RowCount())
		return vector.MustFixedColWithTypeCheck[types.Timestamp](bat.Vecs[3])[0]
	}

	utcTS := readTS("t_ts_utc", produce("t_ts_utc"), time.UTC)
	cstTS := readTS("t_ts_cst", produce("t_ts_cst"), time.FixedZone("+08:00", 8*3600))
	require.Equal(t, utcTS, cstTS, "the message instant must not shift with the session zone")
	require.NotZero(t, utcTS)
}
