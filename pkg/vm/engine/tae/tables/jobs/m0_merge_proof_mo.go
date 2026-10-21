//go:build mo_m0

// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
)

func init() {
	activeM0MergeProof = proveM0MergeObject
}

func proveM0MergeObject(
	ctx context.Context,
	schema *catalog.Schema,
	location objectio.Location,
	fs fileservice.FileService,
) (bool, objectio.M0ObjectProof, error) {
	layout, err := catalog.M0AppendLayoutFromSchema(schema)
	if err != nil {
		return false, objectio.M0ObjectProof{}, nil
	}
	if err = objectio.ValidateM0AppendLayout(layout); err != nil {
		return false, objectio.M0ObjectProof{}, nil
	}
	reader, err := objectio.NewObjectReaderWithStr(location.Name().String(), fs)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)
	header, err := reader.ReadHeader(ctx, mp)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	meta, err := reader.ReadAllMeta(ctx, mp)
	if err != nil {
		return true, objectio.M0ObjectProof{}, err
	}
	width, err := objectio.ValidateM0Object(layout, header, meta.MustDataMeta())
	return true, objectio.M0ObjectProof{
		ObjectName:  location.Name().String(),
		WrittenSize: width,
		SchemaSize:  layout.NextColSeqnum,
	}, err
}
