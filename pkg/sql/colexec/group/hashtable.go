// Copyright 2025 Matrix Origin
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

package group

import "github.com/matrixorigin/matrixone/pkg/container/hashtable"

type HashTableState struct {
	HashType     int    // H0, H8, HStr
	KeyWidth     int    // Width of keys
	KeyNullable  bool   // Whether keys can be null
	GroupCount   uint64 // Number of groups
	HashData     []byte // Serialized hash table data
	HasHashTable bool   // Whether hash table exists
}

func (hr *ResHashRelated) Marshal(mtyp int, keyWidth int, keyNullable bool) (*HashTableState, error) {
	state := &HashTableState{
		HashType:    mtyp,
		KeyWidth:    keyWidth,
		KeyNullable: keyNullable,
	}

	if hr.Hash == nil {
		state.HasHashTable = false
		return state, nil
	}

	state.HasHashTable = true
	state.GroupCount = hr.Hash.GroupCount()

	hashData, err := hr.serializeHashTable()
	if err != nil {
		return nil, err
	}
	state.HashData = hashData

	return state, nil
}

func (hr *ResHashRelated) serializeHashTable() ([]byte, error) {
	if hr.Hash == nil {
		return nil, nil
	}
	return hr.Hash.MarshalBinary()
}

func (hr *ResHashRelated) Unmarshal(state *HashTableState) error {
	if !state.HasHashTable {
		hr.Hash = nil
		hr.Itr = nil
		return nil
	}

	if err := hr.BuildHashTable(true, state.HashType == HStr, state.KeyNullable, state.GroupCount); err != nil {
		return err
	}

	if len(state.HashData) > 0 {
		if err := hr.deserializeHashTable(state.HashData); err != nil {
			hr.Free0()
			return err
		}
	}

	return nil
}

func (hr *ResHashRelated) deserializeHashTable(data []byte) error {
	if hr.Hash == nil || len(data) == 0 {
		return nil
	}
	return hr.Hash.UnmarshalBinary(data, hashtable.DefaultAllocator())
}
