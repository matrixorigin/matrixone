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
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/descriptor"
)

var (
	errorMismatchRefcntWithAttributeCnt = errors.New("mismatch refcnts and attribute cnt")
	errorSomeAttributeNamesAreNotInAttributeDesc = errors.New("some attriute names are not in attribute desc")
)

func (tr *  TpeReader) NewFilter() engine.Filter {
	panic("implement me")
}

func (tr *  TpeReader) NewSummarizer() engine.Summarizer {
	panic("implement me")
}

func (tr *  TpeReader) NewSparseFilter() engine.SparseFilter {
	panic("implement me")
}

func (tr *  TpeReader) Read(refCnts []uint64, attrs []string) (*batch.Batch, error) {
	if tr.isDumpReader {
		return nil, nil
	}
	if len(refCnts) != len(attrs) {
		return nil,errorMismatchRefcntWithAttributeCnt
	}

	var attrDescs []*descriptor.AttributeDesc
	for _, attr := range attrs {
		for i2, tta := range tr.tableDesc.Attributes {
			if tta.Name == attr {
				attrDescs = append(attrDescs,&tr.tableDesc.Attributes[i2])
			}
		}
	}

	if len(attrDescs) != len(attrs) {
		return nil, errorSomeAttributeNamesAreNotInAttributeDesc
	}

	var bat *batch.Batch
	var err error

	bat, tr.prefix, tr.prefixLen, err = tr.computeHandler.Read(tr.dbDesc, tr.tableDesc, &tr.tableDesc.Primary_index, attrDescs, tr.prefix, tr.prefixLen)
	if err != nil {
		return nil, err
	}
	for i, ref := range refCnts {
		bat.Vecs[i].Ref = ref
	}
	return bat,err
}