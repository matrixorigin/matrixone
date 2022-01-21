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

package fz

import (
	"os"
)

type TempDir string

func (_ Def) TempDir(
	logger Logger,
	model TempDirModel,
) (
	dir TempDir,
	cleanup Cleanup,
) {

	switch model {

	case "os":
		d, err := os.MkdirTemp(os.TempDir(), "testcube-*")
		ce(err)
		dir = TempDir(d)

		cleanup = func() {
			logger.Info("remove temp dir")
			os.RemoveAll(d)
		}

	case "fuse":
		//TODO

	}

	return
}

type TempDirModel string

func (_ Def) TempDirModel() TempDirModel {
	return "os"
}
