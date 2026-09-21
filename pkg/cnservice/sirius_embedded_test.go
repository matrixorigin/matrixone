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

package cnservice

import (
	"context"
	"errors"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/compile"
	"github.com/matrixorigin/matrixone/pkg/sql/compile/siriusbridge"
	"github.com/stretchr/testify/require"
)

type embeddedRuntimeTestRecorder struct {
	request siriusbridge.Request
	closed  bool
	err     error
}

func (*embeddedRuntimeTestRecorder) Accepting() bool { return true }

func (r *embeddedRuntimeTestRecorder) Close(context.Context) error {
	r.closed = true
	return nil
}

func (r *embeddedRuntimeTestRecorder) Prepare(
	_ context.Context,
	request siriusbridge.Request,
) (*siriusbridge.Query, error) {
	r.request = request
	return nil, r.err
}

func TestEmbeddedSiriusConfigurationDoesNotRequireFlight(t *testing.T) {
	config := SiriusConfig{Backend: "embedded", NativeConfigPath: "sirius.conf"}
	if err := validateSiriusEmbeddedConfig(&config); err != nil {
		t.Fatal(err)
	}
	if config.InputMode != "mo" || config.GPUStreams != 2 || config.MaxWaitingQueries != 16 {
		t.Fatalf("defaults: %+v", config)
	}
	for _, modify := range []func(*SiriusConfig){
		func(c *SiriusConfig) { c.InputMode = "tae" },
		func(c *SiriusConfig) { c.NativeConfigPath = "" },
		func(c *SiriusConfig) { c.GPUStreams = 129 },
		func(c *SiriusConfig) { c.MaxWaitingQueries = 17 },
		func(c *SiriusConfig) { c.BenchmarkNoGC = true },
	} {
		invalid := config
		modify(&invalid)
		if validateSiriusEmbeddedConfig(&invalid) == nil {
			t.Fatalf("accepted %+v", invalid)
		}
	}
}

func TestEmbeddedSiriusBackendMapsRequestAndDelegates(t *testing.T) {
	wantErr := errors.New("prepare failed")
	recorder := &embeddedRuntimeTestRecorder{err: wantErr}
	backend := &embeddedBackend{native: recorder}
	require.True(t, backend.Accepting())
	require.False(t, backend.CanFallbackBeforeVisibility(wantErr))
	require.ErrorContains(t, backend.Reconcile(1, nil, nil), "no external execution")
	require.NoError(t, backend.Close(t.Context()))
	require.True(t, recorder.closed)

	producerCalls := 0
	producer := func(context.Context, compile.SiriusInput) error {
		producerCalls++
		return nil
	}
	request := compile.SiriusPrepareRequest{
		AccountID:   7,
		QueryID:     []byte("query"),
		Plan:        []byte("plan"),
		OutputTypes: []planpb.Type{{Id: int32(types.T_int64), Width: 8, NotNullable: true}},
		Headings:    []string{"answer"},
		Reads: []compile.SiriusReadDescriptor{{
			BindingID: 9,
			Database:  "db",
			Table:     "table",
			Schema:    "schema",
			Columns: []compile.SiriusReadColumn{{
				Name: "value", Type: planpb.Type{Id: int32(types.T_int64), Width: 8},
				PhysicalID: 11, Sequence: 12,
			}},
			Producer: producer,
		}},
	}
	execution, err := backend.Prepare(t.Context(), request)
	require.Nil(t, execution)
	require.ErrorIs(t, err, wantErr)
	require.Equal(t, request.AccountID, recorder.request.AccountID)
	require.Equal(t, request.QueryID, recorder.request.QueryID)
	require.Equal(t, request.Plan, recorder.request.Plan)
	require.Equal(t, []siriusbridge.Column{{
		OID: uint32(types.T_int64), Width: 8, Nullable: false, Name: "answer",
	}}, recorder.request.Columns)
	require.Len(t, recorder.request.Reads, 1)
	mapped := recorder.request.Reads[0]
	require.Equal(t, uint64(9), mapped.BindingID)
	require.Equal(t, "db", mapped.Database)
	require.Equal(t, "table", mapped.Table)
	require.Equal(t, "schema", mapped.Schema)
	require.Equal(t, []siriusbridge.ReadColumn{{
		Column:     siriusbridge.Column{OID: uint32(types.T_int64), Width: 8, Nullable: true, Name: "value"},
		PhysicalID: 11, Sequence: 12,
	}}, mapped.Columns)
	require.NotNil(t, mapped.Producer)
	require.NoError(t, mapped.Producer(t.Context(), nil))
	require.Equal(t, 1, producerCalls)
}

func TestEmbeddedSiriusStubFailsClosed(t *testing.T) {
	if siriusbridge.Available() {
		t.Skip("native Sirius build exercises the tagged launcher")
	}
	s := &service{}
	require.ErrorContains(t, s.startEmbeddedSiriusRuntime(t.Context()), "not available")
}
