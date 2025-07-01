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

import (
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Spiller manages spilling data to disk for the group operator.
type Spiller struct {
	fs         fileservice.MutableFileService
	spillFiles []string
	proc       *process.Process
}

func NewSpiller(proc *process.Process) (*Spiller, error) {
	fs, err := proc.GetSpillFileService()
	if err != nil {
		return nil, err
	}
	return &Spiller{
		fs:   fs,
		proc: proc,
	}, nil
}
