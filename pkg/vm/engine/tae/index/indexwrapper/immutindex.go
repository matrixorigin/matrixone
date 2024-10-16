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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type ImmutIndex struct {
	zm       index.ZM
	bf       objectio.BloomFilter
	location objectio.Location
}

func NewImmutIndex(
	zm index.ZM,
	bf objectio.BloomFilter,
	location objectio.Location,
) ImmutIndex {
	return ImmutIndex{
		zm:       zm,
		bf:       bf,
		location: location,
	}
}

func (idx ImmutIndex) BatchDedup(
	ctx context.Context,
	keys containers.Vector,
	keysZM index.ZM,
	rt *dbutils.Runtime,
	isTombstone bool,
	blkID uint32,
) (sels *nulls.Bitmap, err error) {
	var exist bool
	if keysZM.Valid() {
		if exist = idx.zm.FastIntersect(keysZM); !exist {
			// all keys are not in [min, max]. definitely not
			return
		}
	} else {
		if exist = idx.zm.FastContainsAny(keys.GetDownstreamVector()); !exist {
			// all keys are not in [min, max]. definitely not
			return
		}
	}

	// some keys are in [min, max]. check bloomfilter for those keys

	var buf []byte
	if len(idx.bf) > 0 {
		buf = idx.bf.GetBloomFilter(blkID)
	} else {
		var bf objectio.BloomFilter
		if bf, err = objectio.FastLoadBF(
			ctx,
			idx.location,
			false,
			rt.Fs.Service,
		); err != nil {
			return
		}
		buf = bf.GetBloomFilter(blkID)
	}

	var typ uint8
	if isTombstone {
		typ = index.HBF
	} else {
		typ = index.BF
	}
	bfIndex := index.NewEmptyBloomFilterWithType(typ)
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

func (idx ImmutIndex) Dedup(
	ctx context.Context, key any, rt *dbutils.Runtime, blkID uint32,
) (err error) {
	exist := idx.zm.Contains(key)
	// 1. if not in [min, max], key is definitely not found
	if !exist {
		return
	}
	var buf []byte
	if len(idx.bf) > 0 {
		buf = idx.bf.GetBloomFilter(blkID)
	} else {
		var bf objectio.BloomFilter
		if bf, err = objectio.FastLoadBF(
			ctx,
			idx.location,
			false,
			rt.Fs.Service,
		); err != nil {
			return
		}
		buf = bf.GetBloomFilter(blkID)
	}

	bfIndex := index.NewEmptyBloomFilter()
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
