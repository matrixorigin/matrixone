// Copyright 2021 - 2022 Matrix Origin
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

package tnservice

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"
	"github.com/stretchr/testify/require"
)

func TestAvailableStores(t *testing.T) {
	tnStores := []*util.Store{
		util.NewStore("1", 1, 1), // full
		util.NewStore("2", 3, 3), // full
		util.NewStore("3", 1, 3), // 2 slots
		util.NewStore("4", 4, 4), // full
		util.NewStore("5", 1, 4), // 3 slots
	}

	stores := spareStores(tnStores)
	require.Equal(t, 2, len(stores))
}

func TestFilter(t *testing.T) {
	tnStores := []*util.Store{
		util.NewStore("0", 0, 10),
		util.NewStore("1", 1, 10),
		util.NewStore("10", 10, 10),
		util.NewStore("5", 5, 5),
	}

	fullFilter := &filterOutFull{}
	candidates := make([]*util.Store, 0, len(tnStores))
	for _, store := range tnStores {
		if fullFilter.Filter(store) {
			continue
		}
		candidates = append(candidates, store)
	}
	require.Equal(t, 2, len(candidates))
}
