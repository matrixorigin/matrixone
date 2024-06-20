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

import "github.com/matrixorigin/matrixone/pkg/hakeeper/checkers/util"

// filterOutFull filter out full TN store
type filterOutFull struct {
}

func (f *filterOutFull) Filter(store *util.Store) bool {
	return store.Length >= store.Capacity
}

// availableStores selects available tn stores.
func spareStores(working []*util.Store) []*util.Store {
	return util.FilterStore(working, []util.IFilter{
		&filterOutFull{},
	})
}
