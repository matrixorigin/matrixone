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

package dnservice

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAvailableStores(t *testing.T) {
	dnStores := []*dnStore{
		newDnStore("1", 1, 1), // full
		newDnStore("2", 3, 3), // full
		newDnStore("3", 1, 3), // 2 slots
		newDnStore("4", 4, 4), // full
		newDnStore("5", 1, 4), // 3 slots
	}

	stores := spareStores(dnStores)
	require.Equal(t, 2, len(stores))
}

func TestFilter(t *testing.T) {
	dnStores := []*dnStore{
		newDnStore("0", 0, 10),
		newDnStore("1", 1, 10),
		newDnStore("10", 10, 10),
		newDnStore("5", 5, 5),
	}

	fullFilter := &filterOutFull{}
	candidates := make([]*dnStore, 0, len(dnStores))
	for _, store := range dnStores {
		if fullFilter.Filter(store) {
			continue
		}
		candidates = append(candidates, store)
	}
	require.Equal(t, 2, len(candidates))
}
