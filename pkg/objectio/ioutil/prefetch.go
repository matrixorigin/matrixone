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

package ioutil

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

type PrefetchType uint8

const (
	PrefetchFileType PrefetchType = iota
	PrefetchMetaType
)

// PrefetchParams is the parameter of the executed IoPipeline.PrefetchParams, which
// provides the merge function, which can merge the PrefetchParams requests of
// multiple blocks in an object/file
type PrefetchParams struct {
	fs  fileservice.FileService
	key objectio.Location
	typ PrefetchType
}

func BuildPrefetchParams(service fileservice.FileService, key objectio.Location) (PrefetchParams, error) {
	pp := buildPrefetchParams(service, key)
	return pp, nil
}

type fetchParams struct {
	idxes  []uint16
	typs   []types.Type
	blk    uint16
	reader *objectio.ObjectReader
}

func buildPrefetchParams(service fileservice.FileService, key objectio.Location) PrefetchParams {
	return PrefetchParams{
		fs:  service,
		key: key,
	}
}

func mergePrefetch(processes []PrefetchParams) map[string]PrefetchParams {
	pc := make(map[string]PrefetchParams)
	for _, p := range processes {
		pc[p.key.Name().String()] = p
	}
	return pc
}

func Prefetch(
	sid string,
	service fileservice.FileService,
	key objectio.Location,
) error {
	params, err := BuildPrefetchParams(service, key)
	if err != nil {
		return err
	}
	params.typ = PrefetchFileType
	return MustGetPipeline(sid).Prefetch(params)
}

func PrefetchMeta(
	sid string,
	service fileservice.FileService,
	key objectio.Location,
) error {
	params, err := BuildPrefetchParams(service, key)
	if err != nil {
		return err
	}
	params.typ = PrefetchMetaType
	return MustGetPipeline(sid).Prefetch(params)
}
