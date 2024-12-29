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

package checkpoint

import (
	"context"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ckputil"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"go.uber.org/zap"
)

func MakeMetadataBatch() *batch.Batch {
	return batch.NewWithSchema(
		false,
		CheckpointSchema.Attrs(),
		CheckpointSchema.Types(),
	)
}

func MakeCKPMetaDirReader(
	ctx context.Context,
	sid string,
	verbose int,
	fs fileservice.FileService,
) (*CKPMetaReader, error) {
	var (
		names []string
		err   error
	)
	if names, err = ckputil.ListCKPMetaNames(ctx, fs); err != nil {
		return nil, err
	}
	return NewCKPMetaReader(
		sid, ioutil.GetCheckpointDir(), names, verbose, fs,
	), nil
}

func NewCKPMetaReader(
	sid string,
	dir string,
	files []string,
	verbose int,
	fs fileservice.FileService,
) *CKPMetaReader {
	return &CKPMetaReader{
		sid:     sid,
		dir:     dir,
		files:   files,
		verbose: verbose,
		fs:      fs,
	}
}

type CKPMetaReader struct {
	sid     string
	dir     string
	files   []string
	idx     int
	verbose int
	fs      fileservice.FileService
}

func (r *CKPMetaReader) Next(
	ctx context.Context,
	mp *mpool.MPool,
) (bats []*batch.Batch, release func(), err error) {
	if r.idx >= len(r.files) {
		err = moerr.GetOkStopCurrRecur()
		return
	}
	var (
		now  = time.Now()
		name = r.files[r.idx]
	)
	defer func() {
		logger := logutil.Error
		if err == nil {
			r.idx++
			logger = logutil.Info
		}
		var (
			allocated int
			rows      int
		)
		for _, bat := range bats {
			rows += bat.RowCount()
			allocated += bat.Allocated()
		}
		logger(
			"Read-CKP-MF",
			zap.Error(err),
			zap.Int("bat-cnt", len(bats)),
			zap.Int("rows", rows),
			zap.Int("allocated", allocated),
			zap.String("name", name),
			zap.Duration("cost", time.Since(now)),
		)
	}()

	select {
	case <-ctx.Done():
		err = context.Cause(ctx)
		return
	default:
	}

	fname := ioutil.MakeFullName(r.dir, name)

	var reader *blockio.BlockReader
	if reader, err = blockio.NewFileReader(
		r.sid,
		r.fs,
		fname,
	); err != nil {
		return
	}

	bats, release, err = reader.LoadAllColumns(
		ctx, nil, mp,
	)
	return
}

func (r *CKPMetaReader) Close() {
	r.fs = nil
	r.files = nil
	r.idx = 0
}
