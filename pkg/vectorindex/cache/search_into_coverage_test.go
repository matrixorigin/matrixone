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
	"errors"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/vectorindex"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/sqlexec"
	"github.com/stretchr/testify/require"
)

type searchIntoError struct {
	MockSearchSearchError
}

func (*searchIntoError) SearchInto(*sqlexec.SqlProcess, any, vectorindex.RuntimeConfig, *vectorindex.SearchOutput) error {
	return errors.New("search into error")
}

func TestVectorIndexSearchIntoStatusAndDelegation(t *testing.T) {
	mock := &MockSearch{}
	s := &VectorIndexSearch{Algo: mock}
	s.Cond = sync.NewCond(s.Mutex.RLocker())
	require.NoError(t, s.Load(nil))
	require.NoError(t, s.SearchInto(nil, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{}))

	s.Status.Store(STATUS_DESTROYED)
	require.ErrorContains(t, s.SearchInto(nil, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{}), "Index destroyed")
	s.Status.Store(STATUS_ERROR)
	require.ErrorContains(t, s.SearchInto(nil, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{}), "Load index error")
	s.Destroy()
}

func TestVectorIndexCacheSearchIntoSuccessAndError(t *testing.T) {
	c := NewVectorIndexCache()
	require.NoError(t, c.SearchInto(nil, "ok", &MockSearch{}, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{}))
	require.NoError(t, c.SearchInto(nil, "ok", &MockSearch{}, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{}))
	require.Error(t, c.SearchInto(nil, "bad", &searchIntoError{}, nil, vectorindex.RuntimeConfig{}, &vectorindex.SearchOutput{}))
	c.Destroy()
}
