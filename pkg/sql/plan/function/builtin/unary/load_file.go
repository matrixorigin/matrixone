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
	"errors"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func LoadFile(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.New(types.T_varchar, 0, 0, 0)
	vec := vector.New(resultType)
	const blobsize = 65536 // 2^16-1
	Filepath := vector.GetStrColumn(inputVector).GetString(0)
	fs := proc.FileService
	r, err := ReadFromFile(Filepath, fs)
	if err != nil {
		return nil, err
	}
	ctx, err := io.ReadAll(r)
	defer r.Close()
	if len(ctx) > blobsize {
		return nil, errors.New("Data too long for blob")
	}
	if err := vec.Append(ctx, proc.GetMheap()); err != nil {
		return nil, err
	}
	return vec, nil
}

func ReadFromFile(Filepath string, fs fileservice.FileService) (io.ReadCloser, error) {
	var r io.ReadCloser
	// index := strings.LastIndex(Filepath, "/")
	// dir, file := "", Filepath
	// if index != -1 {
	// 	dir = string([]byte(Filepath)[0:index])
	// 	file = string([]byte(Filepath)[index+1:])
	// }

	// fs, err := fileservice.NewLocalETLFS("etl", dir)
	// if err != nil {
	// 	return nil, err
	// }
	ctx := context.Background()
	vec := fileservice.IOVector{
		FilePath: Filepath,
		Entries: []fileservice.IOEntry{
			0: {
				Offset:            0,
				Size:              -1,
				ReadCloserForRead: &r,
			},
		},
	}
	err := fs.Read(ctx, &vec)
	if err != nil {
		return nil, err
	}
	return r, nil
}
