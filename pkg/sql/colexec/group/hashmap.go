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

import "github.com/matrixorigin/matrixone/pkg/common/moerr"

func (hr *ResHashRelated) Size() int64 {
	if hr.Hash == nil {
		return 0
	}
	return hr.Hash.Size()
}

type HashTableMetadata struct {
	IsStrHash   bool
	KeyNullable bool
	GroupCount  uint64
	KeyWidth    int
}

func (hr *ResHashRelated) MarshalHashTable() ([]byte, error) {
	if hr.Hash == nil {
		return nil, nil
	}

	metadata := HashTableMetadata{
		IsStrHash:   false, // will be set based on actual type
		KeyNullable: false, // will be determined from context
		GroupCount:  hr.Hash.GroupCount(),
		KeyWidth:    0, // will be set from context
	}

	// We'll marshal just the metadata since we can rebuild the hash table
	// from the spilled group batches
	buf := make([]byte, 8+8+8+8) // bool + bool + uint64 + int (padded)
	offset := 0

	if metadata.IsStrHash {
		buf[offset] = 1
	}
	offset++

	if metadata.KeyNullable {
		buf[offset] = 1
	}
	offset++

	// Pad to 8-byte boundary
	offset = 8

	// GroupCount (uint64)
	for i := 0; i < 8; i++ {
		buf[offset+i] = byte(metadata.GroupCount >> (i * 8))
	}
	offset += 8

	// KeyWidth (int64)
	keyWidth := int64(metadata.KeyWidth)
	for i := 0; i < 8; i++ {
		buf[offset+i] = byte(keyWidth >> (i * 8))
	}

	return buf, nil
}

func (hr *ResHashRelated) UnmarshalHashTable(data []byte, isStrHash bool, keyNullable bool, keyWidth int) error {
	if len(data) == 0 {
		return nil
	}

	if len(data) < 24 {
		return moerr.NewInternalErrorNoCtx("invalid hash table metadata size")
	}

	// Extract GroupCount
	groupCount := uint64(0)
	for i := 0; i < 8; i++ {
		groupCount |= uint64(data[8+i]) << (i * 8)
	}

	// Rebuild hash table with the same configuration
	return hr.BuildHashTable(true, isStrHash, keyNullable, groupCount)
}

func (hr *ResHashRelated) GetHashTableConfig() (isStrHash bool, keyNullable bool, groupCount uint64) {
	if hr.Hash == nil {
		return false, false, 0
	}
	// We'll need to store this information when the hash table is created
	// For now, return what we can determine
	return false, false, hr.Hash.GroupCount()
}
