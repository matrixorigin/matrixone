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

package cache

import (
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type retryingLoadSearch struct {
	loads    atomic.Int32
	destroys atomic.Int32
}

func (m *retryingLoadSearch) Search(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig) (any, []float64, error) {
	return []int64{1}, []float64{1}, nil
}

func (m *retryingLoadSearch) SearchFloat32(_ *sqlexec.SqlProcess, _ any, _ vectorindex.RuntimeConfig, outKeys []int64, outDists []float32) error {
	outKeys[0] = 1
	outDists[0] = 1
	return nil
}

func (m *retryingLoadSearch) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return nil
}

func (m *retryingLoadSearch) Load(*sqlexec.SqlProcess) error {
	if m.loads.Add(1) == 1 {
		return moerr.NewInvalidStateNoCtx("load superseded")
	}
	return nil
}

func (m *retryingLoadSearch) Destroy() {
	m.destroys.Add(1)
}

func TestVectorIndexCacheSearchRetriesSupersededLoad(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &retryingLoadSearch{}
	keys, distances, err := c.Search(nil, "key", algo, nil, vectorindex.RuntimeConfig{})
	require.NoError(t, err)
	require.Equal(t, []int64{1}, keys)
	require.Equal(t, []float64{1}, distances)
	require.Equal(t, int32(2), algo.loads.Load())
	require.Equal(t, int32(1), algo.destroys.Load(), "the failed mapped entry must be destroyed before retry")

	c.Remove("key")
	require.Equal(t, int32(2), algo.destroys.Load())
}

func TestVectorIndexCacheSearchIntoRetriesSupersededLoad(t *testing.T) {
	c := NewVectorIndexCache()
	algo := &retryingLoadSearch{}
	err := c.SearchInto(nil, "key", algo, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{})
	require.NoError(t, err)
	require.Equal(t, int32(2), algo.loads.Load())
	require.Equal(t, int32(1), algo.destroys.Load(), "the failed mapped entry must be destroyed before retry")

	c.Remove("key")
	require.Equal(t, int32(2), algo.destroys.Load())
}
