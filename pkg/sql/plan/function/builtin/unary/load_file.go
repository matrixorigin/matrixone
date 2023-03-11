// Copyright 2022 Matrix Origin
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

package unary

import (
	"context"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	blobsize = 65536 // 2^16-1
)

func LoadFile(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := ivecs[0]
	rtyp := types.T_text.ToType()
	if inputVector.IsConstNull() {
		return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil

	}
	Filepath := vector.MustStrCol(inputVector)[0]
	fs := proc.FileService
	r, err := ReadFromFile(Filepath, fs)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	ctx, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	if len(ctx) > blobsize {
		return nil, moerr.NewInternalError(proc.Ctx, "Data too long for blob")
	}
	var isNull bool
	if len(ctx) == 0 {
		isNull = true
	}
	if isNull {
		return vector.NewConstNull(rtyp, 1, proc.Mp()), nil
	} else {
		return vector.NewConstBytes(rtyp, ctx, 1, proc.Mp()), nil
	}
}

func ReadFromFile(Filepath string, fs fileservice.FileService) (io.ReadCloser, error) {
	fs, readPath, err := fileservice.GetForETL(fs, Filepath)
	if fs == nil || err != nil {
		return nil, err
	}
	var r io.ReadCloser
	ctx := context.TODO()
	vec := fileservice.IOVector{
		FilePath: readPath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}
	err = fs.Read(ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}
