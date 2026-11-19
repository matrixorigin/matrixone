// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import "github.com/gogo/protobuf/proto"

type catalogTemplateRoot struct {
	table   TableDef
	pkey    PrimaryKeyDef
	cluster ClusterByDef
	view    ViewDef
}

func copyCatalogBytes(src []byte) []byte {
	if src == nil {
		return nil
	}
	return append([]byte{}, src...)
}

// CopyCatalogTableTemplate copies a column-free catalog template into private ownership.
// Cols must be nil; callers populate the result with their own projected columns.
func CopyCatalogTableTemplate(src *TableDef) *TableDef {
	if src.Cols != nil {
		panic("catalog table template contains columns")
	}
	root := new(catalogTemplateRoot)
	dst := &root.table
	*dst = *src
	dst.XXX_sizecache = 0
	if src.XXX_unrecognized != nil {
		dst.XXX_unrecognized = append([]byte{}, src.XXX_unrecognized...)
	}
	if src.TblFunc != nil {
		dst.TblFunc = proto.Clone(src.TblFunc).(*TableFunction)
	}
	if src.Pkey != nil {
		root.pkey = *src.Pkey
		root.pkey.XXX_sizecache = 0
		root.pkey.XXX_unrecognized = copyCatalogBytes(src.Pkey.XXX_unrecognized)
		if src.Pkey.Cols != nil {
			root.pkey.Cols = append([]uint64{}, src.Pkey.Cols...)
		}
		if src.Pkey.Names != nil {
			root.pkey.Names = append([]string{}, src.Pkey.Names...)
		}
		if src.Pkey.Option != nil {
			root.pkey.Option = proto.Clone(src.Pkey.Option).(*IndexOption)
		}
		if src.Pkey.CompPkeyCol != nil {
			root.pkey.CompPkeyCol = proto.Clone(src.Pkey.CompPkeyCol).(*ColDef)
		}
		dst.Pkey = &root.pkey
	}
	if src.ClusterBy != nil {
		root.cluster = *src.ClusterBy
		root.cluster.XXX_sizecache = 0
		root.cluster.XXX_unrecognized = copyCatalogBytes(src.ClusterBy.XXX_unrecognized)
		if src.ClusterBy.CompCbkeyCol != nil {
			root.cluster.CompCbkeyCol = proto.Clone(src.ClusterBy.CompCbkeyCol).(*ColDef)
		}
		dst.ClusterBy = &root.cluster
	}
	if src.ViewSql != nil {
		root.view = *src.ViewSql
		root.view.XXX_sizecache = 0
		root.view.XXX_unrecognized = copyCatalogBytes(src.ViewSql.XXX_unrecognized)
		dst.ViewSql = &root.view
	}
	if src.Partition != nil {
		dst.Partition = proto.Clone(src.Partition).(*Partition)
	}
	if src.Indexes != nil {
		dst.Indexes = make([]*IndexDef, len(src.Indexes))
		for i, v := range src.Indexes {
			if v != nil {
				dst.Indexes[i] = proto.Clone(v).(*IndexDef)
			}
		}
	}
	if src.Fkeys != nil {
		dst.Fkeys = make([]*ForeignKeyDef, len(src.Fkeys))
		for i, v := range src.Fkeys {
			if v != nil {
				dst.Fkeys[i] = proto.Clone(v).(*ForeignKeyDef)
			}
		}
	}
	if src.Checks != nil {
		dst.Checks = make([]*CheckDef, len(src.Checks))
		for i, v := range src.Checks {
			if v != nil {
				dst.Checks[i] = proto.Clone(v).(*CheckDef)
			}
		}
	}
	if src.Props != nil {
		dst.Props = make([]*PropertyDef, len(src.Props))
		values := make([]PropertyDef, len(src.Props))
		for i, v := range src.Props {
			if v != nil {
				values[i] = *v
				values[i].XXX_sizecache = 0
				values[i].XXX_unrecognized = copyCatalogBytes(v.XXX_unrecognized)
				dst.Props[i] = &values[i]
			}
		}
	}
	if src.Defs != nil {
		dst.Defs = make([]*TableDef_DefType, len(src.Defs))
		defs := make([]TableDef_DefType, len(src.Defs))
		wrappers := make([]TableDef_DefType_Properties, len(src.Defs))
		groups := make([]PropertiesDef, len(src.Defs))
		count := 0
		for _, v := range src.Defs {
			if v != nil {
				if w, ok := v.Def.(*TableDef_DefType_Properties); ok && w != nil && w.Properties != nil {
					count += len(w.Properties.Properties)
				}
			}
		}
		values := make([]Property, count)
		pointers := make([]*Property, count)
		offset := 0
		for i, v := range src.Defs {
			if v == nil {
				continue
			}
			w, ok := v.Def.(*TableDef_DefType_Properties)
			if !ok || w == nil {
				dst.Defs[i] = proto.Clone(v).(*TableDef_DefType)
				continue
			}
			defs[i] = *v
			defs[i].XXX_sizecache = 0
			defs[i].XXX_unrecognized = copyCatalogBytes(v.XXX_unrecognized)
			defs[i].Def = &wrappers[i]
			if w.Properties != nil {
				g := w.Properties
				groups[i] = *g
				groups[i].XXX_sizecache = 0
				groups[i].XXX_unrecognized = copyCatalogBytes(g.XXX_unrecognized)
				if g.Properties != nil {
					n := len(g.Properties)
					groups[i].Properties = pointers[offset : offset+n : offset+n]
					for j, prop := range g.Properties {
						if prop != nil {
							values[offset+j] = *prop
							values[offset+j].XXX_sizecache = 0
							values[offset+j].XXX_unrecognized = copyCatalogBytes(prop.XXX_unrecognized)
							pointers[offset+j] = &values[offset+j]
						}
					}
					offset += n
				}
				wrappers[i].Properties = &groups[i]
			}
			dst.Defs[i] = &defs[i]
		}
	}
	if src.RefChildTbls != nil {
		dst.RefChildTbls = append([]uint64{}, src.RefChildTbls...)
	}
	if src.Name2ColIndex != nil {
		dst.Name2ColIndex = make(map[string]int32, len(src.Name2ColIndex))
		for k, v := range src.Name2ColIndex {
			dst.Name2ColIndex[k] = v
		}
	}
	return dst
}

// CopyCatalogType retains unknown fields without sharing their writable storage.
func CopyCatalogType(src Type) Type {
	src.XXX_sizecache = 0
	if src.XXX_unrecognized != nil {
		src.XXX_unrecognized = append([]byte{}, src.XXX_unrecognized...)
	}
	return src
}
