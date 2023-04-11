// Copyright 2021 Matrix Origin
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

package objectio

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
)

func (cm ColumnMeta) GetIndex(ctx context.Context, object *Object, readFunc ReadObjectFunc, m *mpool.MPool) (StaticFilter, error) {
	data := &fileservice.IOVector{
		FilePath: object.name,
		Entries:  make([]fileservice.IOEntry, 1),
	}
	data.Entries[0] = fileservice.IOEntry{
		Offset: int64(cm.BloomFilter().Offset()),
		Size:   int64(cm.BloomFilter().Length()),
	}
	var err error
	data.Entries[0].ToObject = readFunc(int64(cm.BloomFilter().OriginSize()))
	err = object.fs.Read(ctx, data)
	if err != nil {
		return nil, err
	}
	return data.Entries[0].Object.(StaticFilter), nil
}

func (cm ColumnMeta) GetMeta() ColumnMeta {
	return cm
}

func (cm ColumnMeta) MarshalMeta() []byte {
	return cm
}

func (cm ColumnMeta) UnmarshalMate(data []byte) error {
	return nil
}
