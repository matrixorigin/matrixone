// Copyright 2022 Matrix Origin
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

package indexwrapper

import (
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type Loader = func(context.Context) ([]byte, error)

type ImmutIndex struct {
	zm       index.ZM
	bfLoader Loader
}

func NewImmutIndex(
	zm index.ZM,
	bfLoader Loader,
) ImmutIndex {
	return ImmutIndex{
		zm:       zm,
		bfLoader: bfLoader,
	}
}

func (idx ImmutIndex) BatchDedup(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
) (sels *roaring.Bitmap, err error) {
	var exist bool
	if keysZM.Valid() {
		if exist = idx.zm.FastIntersect(keysZM); !exist {
			// all keys are not in [min, max]. definitely not
			return
		}
	} else {
		if exist = idx.zm.FastContainsAny(keys); !exist {
			// all keys are not in [min, max]. definitely not
			return
		}
	}

	// some keys are in [min, max]. check bloomfilter for those keys

	var buf []byte
	if idx.bfLoader != nil {
		// load bloomfilter
		if buf, err = idx.bfLoader(ctx); err != nil {
			return
		}
	} else {
		// no bloomfilter. it is possible duplicate
		err = moerr.GetOkExpectedPossibleDup()
		return
	}

	bfIndex := index.NewEmptyBinaryFuseFilter()
	if err = index.DecodeBloomFilter(bfIndex, buf); err != nil {
		return
	}

	if exist, sels, err = bfIndex.MayContainsAnyKeys(keys); err != nil {
		// check bloomfilter has some unknown error. return err
		err = TranslateError(err)
		return
	} else if !exist {
		// all keys were checked. definitely not
		return
	}

	err = moerr.GetOkExpectedPossibleDup()
	return
}

func (idx ImmutIndex) Dedup(ctx context.Context, key any) (err error) {
	exist := idx.zm.Contains(key)
	// 1. if not in [min, max], key is definitely not found
	if !exist {
		return
	}
	if idx.bfLoader == nil {
		err = moerr.GetOkExpectedPossibleDup()
		return
	}

	buf, err := idx.bfLoader(ctx)
	if err != nil {
		return
	}

	bfIndex := index.NewEmptyBinaryFuseFilter()
	if err = index.DecodeBloomFilter(bfIndex, buf); err != nil {
		return
	}

	v := types.EncodeValue(key, idx.zm.GetType())
	exist, err = bfIndex.MayContainsKey(v)
	// 2. check bloomfilter has some error. return err
	if err != nil {
		err = TranslateError(err)
		return
	}
	// 3. all keys were checked. definitely not
	if !exist {
		return
	}
	err = moerr.GetOkExpectedPossibleDup()
	return
}
