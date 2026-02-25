// Copyright 2023 Matrix Origin
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

package compile

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/defines"
	mock_frontend "github.com/matrixorigin/matrixone/pkg/frontend/test"
	"github.com/matrixorigin/matrixone/pkg/pb/pipeline"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func TestBloomFilterSourcePropagation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedBF := []byte("test_bloom_filter_source_content")

	// 1. Setup Source and Process
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)
	ctx = context.WithValue(ctx, defines.IvfBloomFilter{}, expectedBF)

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("test-txn")}).AnyTimes()

	proc := process.NewTopProcess(ctx, nil, nil, txnOperator, nil, nil, nil, nil, nil, nil)

	s := &Scope{
		Proc: proc,
		DataSource: &Source{
			node: &plan.Node{
				TableDef: &plan.TableDef{
					TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
				},
			},
		},
	}

	// 2. Encode Scope: fillPipeline should extract BF from context into pipeline.Source
	data, err := encodeScope(s)
	require.NoError(t, err)

	// Verify the serialized pipeline.Source contains the BF
	var p pipeline.Pipeline
	err = p.Unmarshal(data)
	require.NoError(t, err)
	require.NotNil(t, p.DataSource)
	require.Equal(t, expectedBF, p.DataSource.BloomFilter, "Serialized Source should carry the Bloom Filter")

	// 3. Decode Scope: generateScope should populate compile.Source.BloomFilter
	remoteProc := process.NewTopProcess(context.Background(), nil, nil, txnOperator, nil, nil, nil, nil, nil, nil)
	decodedScope, err := generateScope(remoteProc, &p, &scopeContext{}, true)
	require.NoError(t, err)
	require.NotNil(t, decodedScope.DataSource)
	require.Equal(t, expectedBF, decodedScope.DataSource.BloomFilter, "Decoded Source should have the Bloom Filter")
}

func TestBloomFilterSourceDirectPropagation(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedBF := []byte("test_direct_bloom_filter_source_content")

	// 1. Setup Source and Process (NO BF in context)
	ctx := defines.AttachAccountId(context.Background(), catalog.System_Account)

	txnOperator := mock_frontend.NewMockTxnOperator(ctrl)
	txnOperator.EXPECT().Snapshot().Return(txn.CNTxnSnapshot{}, nil).AnyTimes()
	txnOperator.EXPECT().Txn().Return(txn.TxnMeta{ID: []byte("test-txn")}).AnyTimes()

	proc := process.NewTopProcess(ctx, nil, nil, txnOperator, nil, nil, nil, nil, nil, nil)

	s := &Scope{
		Proc: proc,
		DataSource: &Source{
			BloomFilter: expectedBF, // BF is set DIRECTLY in DataSource
			node: &plan.Node{
				TableDef: &plan.TableDef{
					TableType: catalog.SystemSI_IVFFLAT_TblType_Entries,
				},
			},
		},
	}

	// 2. Encode Scope: fillPipeline should prioritize s.DataSource.BloomFilter
	data, err := encodeScope(s)
	require.NoError(t, err)

	// Verify the serialized pipeline.Source contains the BF
	var p pipeline.Pipeline
	err = p.Unmarshal(data)
	require.NoError(t, err)
	require.NotNil(t, p.DataSource)
	require.Equal(t, expectedBF, p.DataSource.BloomFilter, "Serialized Source should carry the Bloom Filter (from DataSource)")

	// 3. Decode Scope: generateScope should populate compile.Source.BloomFilter
	remoteProc := process.NewTopProcess(context.Background(), nil, nil, txnOperator, nil, nil, nil, nil, nil, nil)
	decodedScope, err := generateScope(remoteProc, &p, &scopeContext{}, true)
	require.NoError(t, err)
	require.NotNil(t, decodedScope.DataSource)
	require.Equal(t, expectedBF, decodedScope.DataSource.BloomFilter, "Decoded Source should have the Bloom Filter")
}
