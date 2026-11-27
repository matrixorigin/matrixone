// Copyright 2022 Matrix Origin
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

package plan

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// renameIndexColumnParts preserves implicit locators and updates prefix
// metadata together with explicit index keys.
func renameIndexColumnParts(table *TableDef, oldName, newName string) (bool, []*IndexDef, error) {
	changed := false
	updated := make([]*IndexDef, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		explicit := false
		for i, part := range index.Parts {
			if !strings.EqualFold(catalog.ResolveAlias(part), oldName) {
				continue
			}
			if catalog.IsAlias(part) {
				index.Parts[i] = catalog.CreateAlias(newName)
			} else {
				index.Parts[i] = newName
				explicit = true
			}
			changed = true
		}
		if !explicit || oldName == newName || index.IndexAlgoParams == "" {
			continue
		}
		prefixes, err := catalog.IndexPrefixLengthsFromParamsWithError(index.IndexAlgoParams)
		if err != nil {
			return false, nil, err
		}
		var prefixName string
		for name := range prefixes {
			if strings.EqualFold(name, oldName) {
				if prefixName != "" {
					return false, nil, moerr.NewInvalidInputNoCtxf("ambiguous index prefix metadata for column %q", oldName)
				}
				prefixName = name
			}
		}
		if prefixName == "" {
			continue
		}
		if prefixName == newName {
			continue
		}
		for name := range prefixes {
			if name != prefixName && strings.EqualFold(name, newName) {
				return false, nil, moerr.NewInvalidInputNoCtxf("conflicting index prefix metadata for column %q", newName)
			}
		}
		prefixes[newName] = prefixes[prefixName]
		delete(prefixes, prefixName)
		params, err := catalog.IndexParamsStringToMap(index.IndexAlgoParams)
		if err != nil {
			return false, nil, err
		}
		if err = catalog.SetIndexPrefixLengthsInParamMap(params, prefixes); err != nil {
			return false, nil, err
		}
		sessionVars, err := catalog.IndexParamsSessionVars(index.IndexAlgoParams)
		if err != nil {
			return false, nil, err
		}
		index.IndexAlgoParams, err = catalog.IndexParamsMapToJsonStringWithSessionVars(params, sessionVars)
		if err != nil {
			return false, nil, err
		}
		updated = append(updated, index)
	}
	return changed, updated, nil
}

// FindColumn finds column in cols by name.
func FindColumn(cols []*ColDef, name string) *ColDef {
	for _, col := range cols {
		if strings.EqualFold(col.Name, name) {
			return col
		}
	}
	return nil
}

// FindColumnByOriginName finds column in cols by origin name.
func FindColumnByOriginName(cols []*ColDef, originName string) *ColDef {
	for _, col := range cols {
		if col.GetOriginCaseName() == originName {
			return col
		}
	}
	return nil
}

// FindColumn finds column in cols by colId
func FindColumnByColId(cols []*ColDef, colId uint64) *ColDef {
	for _, col := range cols {
		if col.ColId == colId {
			return col
		}
	}
	return nil
}
