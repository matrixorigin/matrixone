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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tpe/tuplecodec"
)

var (
	errorMismatchRefcntWithAttributeCnt = errors.New("mismatch refcnts and attribute cnt")
	errorSomeAttributeNamesAreNotInAttributeDesc = errors.New("some attriute names are not in attribute desc")
	errorInvalidParameters = errors.New("invalid parameters")
	errorDifferentReadAttributesInSameReader = errors.New("different attributes in same reader")
)

func (tr *  TpeReader) NewFilter() engine.Filter {
	return nil
}

func (tr *  TpeReader) NewSummarizer() engine.Summarizer {
	return nil
}

func (tr *  TpeReader) NewSparseFilter() engine.SparseFilter {
	return nil
}

func (tr *  TpeReader) Read(refCnts []uint64, attrs []string) (*batch.Batch, error) {
	if tr.isDumpReader {
		//read nothing
		return nil, nil
	}
	if len(refCnts) == 0 || len(attrs) == 0{
		return nil,errorInvalidParameters
	}
	if len(refCnts) != len(attrs) {
		return nil,errorMismatchRefcntWithAttributeCnt
	}

	attrSet := make(map[string]uint32)
	for _, tableAttr := range tr.tableDesc.Attributes {
		attrSet[tableAttr.Name] = tableAttr.ID
	}

	//check if the attribute is in the relation
	var readAttrs []*descriptor.AttributeDesc
	for _, attr := range attrs {
		if attrID,exist := attrSet[attr]; exist {
			readAttrs = append(readAttrs,&tr.tableDesc.Attributes[attrID])
		}else{
			return nil, errorSomeAttributeNamesAreNotInAttributeDesc
		}
	}

	var bat *batch.Batch
	var err error

	if tr.readCtx == nil {
		tr.readCtx = &tuplecodec.ReadContext{
			DbDesc:                   tr.dbDesc,
			TableDesc:                tr.tableDesc,
			IndexDesc:                &tr.tableDesc.Primary_index,
			ReadAttributesNames:      attrs,
			ReadAttributeDescs:       readAttrs,
			PrefixForScanKey:         nil,
			LengthOfPrefixForScanKey: 0,
		}
	}else{
		//check if these attrs are same as last attrs
		if len(tr.readCtx.ReadAttributesNames) != len(attrs) {
			return nil,errorDifferentReadAttributesInSameReader
		}

		for i := 0; i < len(attrs); i++ {
			if attrs[i] != tr.readCtx.ReadAttributesNames[i] {
				return nil, errorDifferentReadAttributesInSameReader
			}
		}
	}

	bat, err = tr.computeHandler.Read(tr.readCtx)
	if err != nil {
		return nil, err
	}

	//when bat is null,it means no data anymore.
	if bat != nil {
		//attach refCnts
		for i, ref := range refCnts {
			bat.Vecs[i].Ref = ref
		}
	}
	return bat,err
}

func (tr *  TpeReader) DumpRead(refCnts []uint64, attrs []string, opt *batch.DumpOption) (*batch.DumpResult, error) {
	if tr.isDumpReader {
		//read nothing
		return nil, nil
	}
	if len(refCnts) == 0 || len(attrs) == 0{
		return nil,errorInvalidParameters
	}
	if len(refCnts) != len(attrs) {
		return nil,errorMismatchRefcntWithAttributeCnt
	}

	attrSet := make(map[string]uint32)
	for _, tableAttr := range tr.tableDesc.Attributes {
		attrSet[tableAttr.Name] = tableAttr.ID
	}

	//check if the attribute is in the relation
	var readAttrs []*descriptor.AttributeDesc
	for _, attr := range attrs {
		if attrID,exist := attrSet[attr]; exist {
			readAttrs = append(readAttrs,&tr.tableDesc.Attributes[attrID])
		}else{
			return nil, errorSomeAttributeNamesAreNotInAttributeDesc
		}
	}

	var err error

	if tr.readCtx == nil {
		tr.readCtx = &tuplecodec.ReadContext{
			DbDesc:                   tr.dbDesc,
			TableDesc:                tr.tableDesc,
			IndexDesc:                &tr.tableDesc.Primary_index,
			ReadAttributesNames:      attrs,
			ReadAttributeDescs:       readAttrs,
			PrefixForScanKey:         nil,
			LengthOfPrefixForScanKey: 0,
		}
	}else{
		//check if these attrs are same as last attrs
		if len(tr.readCtx.ReadAttributesNames) != len(attrs) {
			return nil,errorDifferentReadAttributesInSameReader
		}

		for i := 0; i < len(attrs); i++ {
			if attrs[i] != tr.readCtx.ReadAttributesNames[i] {
				return nil, errorDifferentReadAttributesInSameReader
			}
		}
	}

	result, err := tr.computeHandler.DumpRead(tr.readCtx, opt)
	if err != nil {
		return nil, err
	}
	return result,err
}