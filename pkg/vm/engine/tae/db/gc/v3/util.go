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

package gc

import (
	"context"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/readutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

func MakeLoadFunc(
	ctx context.Context,
	tail []*batch.Batch,
	objects []objectio.ObjectStats,
	fs fileservice.FileService,
	ts timestamp.Timestamp,
	opts ...readutil.ReaderOption,
) (
	SourerFn,
	func(),
) {
	var (
		cursor int
		reader engine.Reader
	)
	if len(objects) > 0 {
		reader = readutil.SimpleMultiObjectsReader(
			ctx, fs, objects, ts, opts...,
		)
	}
	releaseFn := func() {
		if reader != nil {
			reader.Close()
		}
	}
	return func(
		ctx context.Context, _ []string, _ *plan.Expr, mp *mpool.MPool, bat *batch.Batch,
	) (bool, error) {
		if cursor < len(tail) {
			if _, err := bat.AppendWithCopy(ctx, mp, tail[cursor]); err != nil {
				return false, err
			}
			cursor++
			return false, nil
		}
		if reader != nil {
			return reader.Read(ctx, bat.Attrs, nil, mp, bat)
		}
		return true, nil
	}, releaseFn
}

func MakeGCWindowBuffer(size int) *containers.OneSchemaBatchBuffer {
	return containers.NewOneSchemaBatchBuffer(
		size, ObjectTableAttrs, ObjectTableTypes, false,
	)
}

func DeleteObjects(
	ctx context.Context,
	fs fileservice.FileService,
	objects []objectio.ObjectStats,
) error {
	if len(objects) == 0 {
		return nil
	}
	files := make([]string, 0, len(objects))
	for _, obj := range objects {
		files = append(files, obj.ObjectName().String())
	}
	return fs.Delete(ctx, files...)
}
