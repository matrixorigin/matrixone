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

package ckputil

import (
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
)

func EncodeCluser(
	packer *types.Packer,
	tableId uint64,
	obj *objectio.ObjectId,
) {
	packer.EncodeUint64(tableId)
	packer.EncodeObjectid(obj)
}

var SinkerFactory ioutil.FileSinkerFactory

func init() {
	SinkerFactory = ioutil.NewFSinkerImplFactory(
		TableObjectsSeqnums,
		TableObjectsAttr_Cluster_Idx,
		false,
		false,
		0,
	)
}

func NewSinker(
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...ioutil.SinkerOption,
) *ioutil.Sinker {
	return ioutil.NewSinker(
		TableObjectsAttr_Cluster_Idx,
		TableObjectsAttrs,
		TableObjectsTypes,
		SinkerFactory,
		mp,
		fs,
		opts...,
	)
}
