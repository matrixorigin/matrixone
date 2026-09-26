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

//go:build sirius && sirius_integration && cgo && linux && amd64

package siriusbridge

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	spb "github.com/substrait-io/substrait-protobuf/go/substraitpb"
	"google.golang.org/protobuf/proto"
)

func nativeIntegrationRequest(t *testing.T, id string, producer func(context.Context, *Input) error) Request {
	t.Helper()
	read := &spb.ReadRel{
		BaseSchema: &spb.NamedStruct{Names: []string{"c"}, Struct: &spb.Type_Struct{
			Types: []*spb.Type{{Kind: &spb.Type_I64_{I64: &spb.Type_I64{Nullability: spb.Type_NULLABILITY_REQUIRED}}}},
		}},
		ReadType: &spb.ReadRel_NamedTable_{NamedTable: &spb.ReadRel_NamedTable{Names: []string{"__sirius_embedded_v1", "1"}}},
	}
	plan := &spb.Plan{Version: &spb.Version{MinorNumber: 78}, Relations: []*spb.PlanRel{{RelType: &spb.PlanRel_Root{Root: &spb.RelRoot{
		Names: []string{"c"}, Input: &spb.Rel{RelType: &spb.Rel_Read{Read: read}},
	}}}}}
	data, err := proto.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	column := Column{OID: 23, Name: "c"}
	return Request{AccountID: 42, QueryID: []byte(id), Plan: data, Columns: []Column{column},
		Reads: []Read{{BindingID: 1, Database: "db", Table: "t", Schema: "s", Columns: []ReadColumn{{Column: column, PhysicalID: 7, Sequence: 3}}, Producer: producer}}}
}

func closeNativeQuery(t *testing.T, q *Query) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := q.Close(ctx); err != nil {
		t.Error(err)
	}
}

// This test opts into the real process-owned runtime, not the fake driver seam.
// Missing native configuration is a failure under the integration build tag.
func TestNativeBridgeDataAndCancellation(t *testing.T) {
	config := os.Getenv("MO_SIRIUS_TEST_CONFIG")
	if config == "" {
		t.Fatal("sirius_integration requires MO_SIRIUS_TEST_CONFIG")
	}
	config, err := filepath.Abs(config)
	if err != nil {
		t.Fatal(err)
	}
	// Native telemetry and spill paths must stay inside this test's workspace.
	t.Chdir(t.TempDir())
	runtime, err := New(Config{ConfigPath: config, GPUStreams: 2, MaxWaiting: 16})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})

	t.Run("exact native scalar result", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		var started, released atomic.Int32
		request := nativeIntegrationRequest(t, "go-native-data", func(ctx context.Context, input *Input) error {
			started.Add(1)
			values := []int64{7, -9, 42}
			data := make([]byte, len(values)*8)
			for i, value := range values {
				binary.LittleEndian.PutUint64(data[i*8:], uint64(value))
			}
			return input.Push(ctx, uint32(len(values)), []Vector{{Data: data}})
		})
		request.Release = func(context.Context) error { released.Add(1); return nil }
		query, err := runtime.Prepare(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeNativeQuery(t, query) })
		if started.Load() != 0 {
			t.Fatal("native preparation started an MO producer")
		}
		var got []int64
		err = query.Run(ctx, func(result Result) error {
			if len(result.Vectors) != 1 || result.Vectors[0].Class != 0 {
				return errors.New("native output schema/vector class mismatch")
			}
			vector := result.Vectors[0]
			if len(vector.Data) != int(result.Rows)*8 {
				return errors.New("native int64 output length mismatch")
			}
			for _, word := range vector.Nulls {
				if word != 0 {
					return errors.New("unexpected native NULL")
				}
			}
			for row := uint32(0); row < result.Rows; row++ {
				got = append(got, int64(binary.LittleEndian.Uint64(vector.Data[int(row)*8:])))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
		if !reflect.DeepEqual(got, []int64{-9, 7, 42}) {
			t.Fatalf("native rows: %v", got)
		}
		closeNativeQuery(t, query)
		if started.Load() != 1 || released.Load() != 1 {
			t.Fatalf("producer/release counts: %d/%d", started.Load(), released.Load())
		}
	})

	t.Run("empty batch and constant null input", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		request := nativeIntegrationRequest(t, "go-native-null", func(ctx context.Context, input *Input) error {
			if err := input.Push(ctx, 0, []Vector{{}}); err != nil {
				return err
			}
			return input.Push(ctx, 3, []Vector{{Class: 2}})
		})
		request.Columns[0].Nullable = true
		request.Reads[0].Columns[0].Nullable = true
		var plan spb.Plan
		if err := proto.Unmarshal(request.Plan, &plan); err != nil {
			t.Fatal(err)
		}
		plan.Relations[0].GetRoot().Input.GetRead().BaseSchema.Struct.Types[0].GetI64().Nullability = spb.Type_NULLABILITY_NULLABLE
		request.Plan, err = proto.Marshal(&plan)
		if err != nil {
			t.Fatal(err)
		}
		query, err := runtime.Prepare(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeNativeQuery(t, query) })
		var rows uint32
		if err := query.Run(ctx, func(result Result) error {
			if len(result.Vectors) != 1 {
				return errors.New("constant NULL output schema mismatch")
			}
			for row := uint32(0); row < result.Rows; row++ {
				nulls := result.Vectors[0].Nulls
				if int(row/8) >= len(nulls) || nulls[row/8]&(1<<(row%8)) == 0 {
					return errors.New("constant NULL input lost its validity bit")
				}
			}
			rows += result.Rows
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if rows != 3 {
			t.Fatalf("constant NULL rows: %d", rows)
		}
	})

	t.Run("cancel waiting native result and producer", func(t *testing.T) {
		ctx, deadlineCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer deadlineCancel()
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		entered, exited := make(chan struct{}), make(chan struct{})
		request := nativeIntegrationRequest(t, "go-native-cancel", func(ctx context.Context, _ *Input) error {
			defer close(exited)
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		})
		var released atomic.Int32
		request.Release = func(context.Context) error {
			select {
			case <-exited:
				released.Add(1)
				return nil
			default:
				return errors.New("native ownership released before producer joined")
			}
		}
		query, err := runtime.Prepare(ctx, request)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { closeNativeQuery(t, query) })
		done := make(chan error, 1)
		go func() {
			done <- query.Run(ctx, func(Result) error { return errors.New("unexpected result without producer input") })
		}()
		select {
		case <-entered:
		case <-ctx.Done():
			t.Fatal("producer did not start")
		}
		cancel()
		select {
		case err = <-done:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Run: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("native cancellation did not join")
		}
		closeNativeQuery(t, query)
		if released.Load() != 1 {
			t.Fatalf("release count: %d", released.Load())
		}
	})
}
