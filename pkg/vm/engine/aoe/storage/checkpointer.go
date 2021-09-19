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

package engine

import (
	"errors"
	"matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"
)

var (
	ErrAlreadyExist = errors.New("ckp already done")
)

type checkpointerFactory struct {
	dir string
}

func NewCheckpointerFactory(dir string) *checkpointerFactory {
	factory := &checkpointerFactory{
		dir: dir,
	}
	return factory
}

func (f *checkpointerFactory) Create() *checkpointer {
	ck := &checkpointer{
		factory: f,
	}
	return ck
}

type checkpointer struct {
	factory *checkpointerFactory
	tmpfile string
}

func (ck *checkpointer) PreCommit(res metadata.Resource) error {
	if res == nil {
		logutil.Error("nil res")
		return errors.New("nil res")
	}
	var fname string
	switch res.GetResourceType() {
	case metadata.ResInfo:
		fname = common.MakeInfoCkpFileName(ck.factory.dir, res.GetFileName(), true)
	case metadata.ResTable:
		fname = common.MakeTableCkpFileName(ck.factory.dir, res.GetFileName(), res.GetTableId(), true)
	default:
		panic("not supported")
	}
	// log.Infof("PreCommit CheckPoint: %s", fname)
	if _, err := os.Stat(fname); err == nil {
		return ErrAlreadyExist
	}
	dir := filepath.Dir(fname)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
	}
	w, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer w.Close()
	err = res.Serialize(w)
	if err != nil {
		return err
	}
	ck.tmpfile = fname
	return nil
}

func (ck *checkpointer) Commit(res metadata.Resource) error {
	if len(ck.tmpfile) == 0 {
		return errors.New("Cannot Commit checkpoint, should do PreCommit before")
	}
	fname, err := common.FilenameFromTmpfile(ck.tmpfile)
	if err != nil {
		return err
	}
	// log.Infof("Commit CheckPoint: %s", fname)
	err = os.Rename(ck.tmpfile, fname)
	return err
}

func (ck *checkpointer) Load() error {
	// TODO
	return nil
}
