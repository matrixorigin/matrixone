// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package table_function

import (
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func metaScanPrepare(_ *process.Process, arg *Argument) error {
	return nil
}

func metaScanCall(_ int, proc *process.Process, arg *Argument) (bool, error) {
	var (
		err  error
		rbat *batch.Batch
	)
	defer func() {
		if err != nil && rbat != nil {
			rbat.Clean(proc.Mp())
		}
	}()
	bat := proc.InputBatch()
	if bat == nil {
		return true, nil
	}
	v, err := colexec.EvalExpr(bat, proc, arg.Args[0])
	if err != nil {
		return false, err
	}
	uuid := vector.MustTCols[types.Uuid](v)[0]
	// get file size
	path := catalog.BuildQueryResultMetaPath(proc.SessionInfo.Account, uuid.ToString())
	e, err := proc.FileService.StatFile(proc.Ctx, path)
	if err != nil {
		if moerr.IsMoErrCode(err, moerr.ErrFileNotFound) {
			return false, moerr.NewQueryIdNotFound(proc.Ctx, uuid.ToString())
		}
		return false, err
	}
	// read meta's meta
	reader, err := objectio.NewObjectReader(path, proc.FileService)
	if err != nil {
		return false, err
	}
	bs, err := reader.ReadAllMeta(proc.Ctx, e.Size, proc.Mp())
	if err != nil {
		return false, err
	}
	var idxs []uint16
	for i, name := range catalog.MetaColNames {
		for _, attr := range arg.Attrs {
			if name == attr {
				idxs = append(idxs, uint16(i))
			}
		}
	}
	// read meta's data
	iov, err := reader.Read(proc.Ctx, bs[0].GetExtent(), idxs, proc.Mp())
	if err != nil {
		return false, err
	}
	rbat = batch.NewWithSize(len(idxs))
	rbat.SetAttributes(catalog.MetaColNames)
	rbat.Cnt = 1
	for i, e := range iov.Entries {
		rbat.Vecs[i] = vector.New(catalog.MetaColTypes[idxs[i]])
		if err = rbat.Vecs[i].Read(e.Object.([]byte)); err != nil {
			return false, err
		}
	}
	rbat.InitZsOne(1)
	proc.SetInputBatch(rbat)
	return false, nil
}
