package frontend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/dispatch"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/merge"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/value_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// The source is synthetic; Dispatch, spool, Merge, CSV formatting and pipe writes
// are real production implementations from the checked-out revision.
type csvDumpSource struct {
	*value_scan.ValueScan
	batches []*batch.Batch
	next    int
}

func (s *csvDumpSource) Call(*process.Process) (vm.CallResult, error) {
	r := vm.NewCallResult()
	if s.next == len(s.batches) {
		r.Status = vm.ExecStop
		return r, nil
	}
	r.Batch = s.batches[s.next]
	s.next++
	return r, nil
}

func csvDumpConsume(proc *process.Process, m *merge.Merge, ep *ExportConfig, written chan<- int, done chan<- error) {
	var result error
	defer func() { _ = ep.AsyncWriter.CloseWithError(result); done <- result }()
	// constructByte needs only the session's memory pool for integer vectors.
	ses := &Session{feSessionImpl: feSessionImpl{pool: proc.Mp()}}
	rows := 0
	for {
		r, err := m.Call(proc)
		if err != nil {
			result = err
			return
		}
		if r.Batch == nil {
			return
		}
		copied, err := r.Batch.Dup(proc.Mp())
		if err != nil {
			result = err
			return
		}
		index := ep.Index.Add(1)
		constructByte(proc.Ctx, ses, copied, index, ep.ByteChan, ep)
		if err = exportDataFromBatchToCSVFile(ep); err != nil {
			result = err
			return
		}
		rows += r.Batch.RowCount()
		written <- rows
	}
}

// Observe entry into the actual CSV write. The test keeps the pipe reader
// paused, so the write cannot complete until the test releases it.
func csvDumpObserveWrite(t *testing.T, ep *ExportConfig) <-chan struct{} {
	t.Helper()
	entered := make(chan struct{})
	var once sync.Once
	original := Write
	Write = func(config *ExportConfig, data []byte) (int, error) {
		if config == ep {
			once.Do(func() { close(entered) })
		}
		return original(config, data)
	}
	t.Cleanup(func() { Write = original })
	return entered
}

func TestCSVDumpBackpressureTerminalWhitebox(t *testing.T) {
	for _, pauseThroughReset := range []bool{false, true} {
		name := "resume_before_reset"
		if pauseThroughReset {
			name = "resume_after_reset"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			proc := testutil.NewProcess(t)
			proc.BuildPipelineContext(context.Background())
			defer proc.Free()
			defer proc.Cancel(nil)
			source := &csvDumpSource{ValueScan: &value_scan.ValueScan{}}
			source.OpAnalyzer = process.NewAnalyzer(0, false, false, "csv-source")
			for i := int64(1); i <= 2; i++ {
				b := batch.NewWithSize(1)
				b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(b.Vecs[0], i, false, proc.Mp()))
				b.SetRowCount(1)
				source.batches = append(source.batches, b)
				defer b.Clean(proc.Mp())
			}
			reg := &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)}
			proc.Reg.MergeReceivers = []*process.WaitRegister{reg}
			d := &dispatch.Dispatch{FuncId: dispatch.SendToAllLocalFunc, LocalRegs: []*process.WaitRegister{reg}}
			d.AppendChild(source)
			require.NoError(t, d.Prepare(proc))
			m := &merge.Merge{}
			require.NoError(t, m.Prepare(proc))
			defer d.CleanupDeferredSpool()

			reader, writer := io.Pipe()
			defer reader.Close()
			defer writer.Close()
			ep := &ExportConfig{userConfig: &tree.ExportParam{Fields: &tree.Fields{EnclosedBy: &tree.EnclosedBy{}}}, AsyncWriter: writer, Symbol: [][]byte{[]byte("\n")}, ColumnFlag: []bool{false}}
			ep.init()
			file, err := os.Create(filepath.Join(dir, "dump.csv"))
			require.NoError(t, err)
			defer file.Close()
			gate := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(gate) }) }
			defer release()
			readDone := make(chan error, 1)
			go func() { <-gate; _, err := io.Copy(file, reader); readDone <- err }()
			entered := csvDumpObserveWrite(t, ep)
			// One batch is held by Merge while CSV output is paused. The second fills
			// the remaining spool slot and the one-element notification channel.
			_, err = d.Call(proc)
			require.NoError(t, err)
			written := make(chan int, 2)
			consumerDone := make(chan error, 1)
			go csvDumpConsume(proc, m, ep, written, consumerDone)
			joined := false
			defer func() {
				proc.Cancel(nil)
				release()
				_ = reader.Close()
				_ = writer.Close()
				if !joined {
					select {
					case <-consumerDone:
					case <-time.After(5 * time.Second):
						t.Error("consumer cleanup did not finish")
					}
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("CSV write not entered")
			}
			_, err = d.Call(proc)
			require.NoError(t, err)
			r, err := d.Call(proc)
			require.NoError(t, err)
			require.Equal(t, vm.ExecStop, r.Status)
			require.Equal(t, 1, len(reg.Ch2))

			waitWritten := func() {
				for want := 1; want <= 2; want++ {
					select {
					case got := <-written:
						require.Equal(t, want, got)
					case <-time.After(5 * time.Second):
						t.Fatal("CSV did not resume")
					}
				}
			}
			if !pauseThroughReset {
				release()
				waitWritten()
			}
			d.Reset(proc, false, nil)
			if pauseThroughReset {
				release()
				waitWritten()
			}
			require.NoError(t, proc.Ctx.Err())
			select {
			case err = <-consumerDone:
				joined = true
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("consumer did not terminate")
			}
			select {
			case err = <-readDone:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("CSV reader did not terminate")
			}
			require.NoError(t, file.Close())
			data, err := os.ReadFile(filepath.Join(dir, "dump.csv"))
			require.NoError(t, err)
			require.True(t, bytes.Equal(data, []byte("1\n2\n")), "unexpected CSV: %q", data)
		})
	}
}

// A failed destination must break the backpressure cycle without turning the
// failed export into success, losing the original I/O error, or freeing a batch
// still owned by Merge. The queue and both spool slots are occupied at failure.
func TestCSVDumpFailedWriterWhitebox(t *testing.T) {
	for _, name := range []string{"destination_error", "query_cancel_and_destination_error", "producer_end_before_destination_error"} {
		t.Run(name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.BuildPipelineContext(context.Background())
			defer proc.Free()
			defer proc.Cancel(nil)
			source := &csvDumpSource{ValueScan: &value_scan.ValueScan{}}
			source.OpAnalyzer = process.NewAnalyzer(0, false, false, "csv-failure-source")
			for i := int64(1); i <= 2; i++ {
				b := batch.NewWithSize(1)
				b.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				require.NoError(t, vector.AppendFixed(b.Vecs[0], i, false, proc.Mp()))
				b.SetRowCount(1)
				source.batches = append(source.batches, b)
			}
			defer func() {
				for _, b := range source.batches {
					b.Clean(proc.Mp())
				}
			}()
			reg := &process.WaitRegister{Ch2: make(chan process.PipelineSignal, 1)}
			proc.Reg.MergeReceivers = []*process.WaitRegister{reg}
			d := &dispatch.Dispatch{FuncId: dispatch.SendToAllLocalFunc, LocalRegs: []*process.WaitRegister{reg}}
			d.AppendChild(source)
			require.NoError(t, d.Prepare(proc))
			m := &merge.Merge{}
			require.NoError(t, m.Prepare(proc))
			reader, writer := io.Pipe()
			defer reader.Close()
			defer writer.Close()
			ep := &ExportConfig{userConfig: &tree.ExportParam{Fields: &tree.Fields{EnclosedBy: &tree.EnclosedBy{}}}, AsyncWriter: writer, Symbol: [][]byte{[]byte("\n")}, ColumnFlag: []bool{false}}
			ep.init()
			entered := csvDumpObserveWrite(t, ep)
			_, err := d.Call(proc)
			require.NoError(t, err)
			written := make(chan int, 2)
			done := make(chan error, 1)
			go csvDumpConsume(proc, m, ep, written, done)
			joined := false
			defer func() {
				proc.Cancel(nil)
				_ = reader.Close()
				_ = writer.Close()
				if !joined {
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("failed export goroutine did not exit")
					}
				}
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("CSV write not entered")
			}
			_, err = d.Call(proc)
			require.NoError(t, err)
			require.Len(t, reg.Ch2, 1)
			failure := fmt.Errorf("injected CSV destination failure")
			if name == "producer_end_before_destination_error" {
				r, err := d.Call(proc)
				require.NoError(t, err)
				require.Equal(t, vm.ExecStop, r.Status)
				d.Reset(proc, false, nil)
			}
			if name == "query_cancel_and_destination_error" {
				proc.Cancel(failure)
			}
			require.NoError(t, reader.CloseWithError(failure))
			select {
			case err = <-done:
				joined = true
				require.ErrorIs(t, err, failure)
			case <-time.After(5 * time.Second):
				t.Fatal("failed destination left CSV blocked")
			}
			require.Empty(t, written, "failed write must not count a completed row")
			// Abort reclaims pending data, while Merge still owns its current row.
			if name != "producer_end_before_destination_error" {
				d.Reset(proc, true, failure)
			}
			proc.Cancel(failure)
			m.Reset(proc, true, failure)
			d.CleanupDeferredSpool()
			d.CleanupDeferredSpool()
			for _, b := range source.batches {
				b.Clean(proc.Mp())
			}
			require.Zero(t, proc.Mp().CurrNB(), "failed export retained batch memory")
		})
	}
}
