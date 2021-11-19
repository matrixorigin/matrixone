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

package plan

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/util"
	"matrixone/pkg/sql/viewexec/transformer"
)

func (qry *Query) backFill() {
	qry.VarsMap = make(map[string]int)
	for _, rn := range qry.Rels {
		rel := qry.RelsMap[rn]
		for _, attr := range rel.Attrs {
			qry.VarsMap[attr]++
		}
		for _, agg := range rel.Aggregations {
			_, name := util.SplitTableAndColumn(agg.Alias)
			qry.VarsMap[name]++
			if agg.Op == transformer.StarCount {
				agg.Name = rel.Attrs[0]
				rel.AttrsMap[agg.Name].Ref++
			}
		}
		for _, e := range rel.ProjectionExtends {
			_, name := util.SplitTableAndColumn(e.Alias)
			qry.VarsMap[name]++
		}
	}
	for _, e := range qry.ProjectionExtends {
		_, name := util.SplitTableAndColumn(e.Alias)
		qry.VarsMap[name]++
	}
	qry.reduce()
}

// If flg is set, then it will increase the reference count
// 	. only the original attributes will be looked up
func (qry *Query) getAttribute0(flg bool, col string) ([]string, *types.Type, error) {
	var typ *types.Type
	var names []string

	tbl, name := util.SplitTableAndColumn(col)
	if len(tbl) > 0 {
		for i := 0; i < len(qry.Rels); i++ {
			if qry.Rels[i] == tbl {
				rel := qry.RelsMap[tbl]
				if attr, ok := rel.AttrsMap[name]; ok {
					if flg {
						attr.IncRef()
					}
					if typ == nil {
						typ = &attr.Type
					}
					names = append(names, qry.Rels[i])
				}
			}
		}
	} else {
		for i := 0; i < len(qry.Rels); i++ {
			rel := qry.RelsMap[qry.Rels[i]]
			if attr, ok := rel.AttrsMap[name]; ok {
				if flg {
					attr.IncRef()
				}
				if typ == nil {
					typ = &attr.Type
				}
				names = append(names, qry.Rels[i])
			}
		}
	}
	return names, typ, nil
}

// If flg is set, then it will increase the reference count
// 	. original attributes will be looked up
//  . projection will be looked up
func (qry *Query) getAttribute1(flg bool, col string) ([]string, *types.Type, error) {
	var typ *types.Type
	var names []string

	tbl, name := util.SplitTableAndColumn(col)
	if len(tbl) > 0 {
		for i := 0; i < len(qry.Rels); i++ {
			if qry.Rels[i] == tbl {
				rel := qry.RelsMap[tbl]
				if j := rel.ExistProjection(name); j >= 0 {
					if flg {
						rel.ProjectionExtends[j].IncRef()
					}
					if typ == nil {
						typ = &types.Type{Oid: rel.ProjectionExtends[j].E.ReturnType()}
					}
					names = append(names, qry.Rels[i])
				} else if attr, ok := rel.AttrsMap[name]; ok {
					if flg {
						attr.IncRef()
					}
					if typ == nil {
						typ = &attr.Type
					}
					names = append(names, qry.Rels[i])
				}
			}
		}
	} else {
		for i := 0; i < len(qry.Rels); i++ {
			rel := qry.RelsMap[qry.Rels[i]]
			if j := rel.ExistProjection(name); j >= 0 {
				if flg {
					rel.ProjectionExtends[j].IncRef()
				}
				if typ == nil {
					typ = &types.Type{Oid: rel.ProjectionExtends[j].E.ReturnType()}
				}
				names = append(names, qry.Rels[i])

			} else if attr, ok := rel.AttrsMap[name]; ok {
				if flg {
					attr.IncRef()
				}
				if typ == nil {
					typ = &attr.Type
				}
				names = append(names, qry.Rels[i])
			}
		}
	}
	return names, typ, nil
}

// If flg is set, then it will increase the reference count
// 	. original attributes will be looked up
//  . projection will be looked up
//  . aggregation will be looke up
func (qry *Query) getAttribute2(flg bool, col string) ([]string, *types.Type, error) {
	var typ *types.Type
	var names []string

	tbl, name := util.SplitTableAndColumn(col)
	if len(tbl) > 0 { // scope of relation
		for i := 0; i < len(qry.Rels); i++ {
			if qry.Rels[i] == tbl {
				rel := qry.RelsMap[tbl]
				if j := rel.ExistAggregation(name); j >= 0 {
					if flg {
						rel.Aggregations[j].IncRef()
					}
					if typ == nil {
						typ = &types.Type{Oid: rel.Aggregations[j].Type}
					}
					names = append(names, qry.Rels[i])
				} else if j := rel.ExistProjection(name); j >= 0 {
					if flg {
						rel.ProjectionExtends[j].IncRef()
					}
					if typ == nil {
						typ = &types.Type{Oid: rel.ProjectionExtends[j].E.ReturnType()}
					}
					names = append(names, qry.Rels[i])
				} else if attr, ok := rel.AttrsMap[name]; ok {
					if flg {
						attr.IncRef()
					}
					if typ == nil {
						typ = &attr.Type
					}
					names = append(names, qry.Rels[i])
				}
			}
		}
	} else {
		for i := 0; i < len(qry.Rels); i++ {
			rel := qry.RelsMap[qry.Rels[i]]
			if j := rel.ExistAggregation(name); j >= 0 {
				if flg {
					rel.Aggregations[j].IncRef()
				}
				if typ == nil {
					typ = &types.Type{Oid: rel.Aggregations[j].Type}
				}
				names = append(names, qry.Rels[i])
			} else if j := rel.ExistProjection(name); j >= 0 {
				if flg {
					rel.ProjectionExtends[j].IncRef()
				}
				if typ == nil {
					typ = &types.Type{Oid: rel.ProjectionExtends[j].E.ReturnType()}
				}
				names = append(names, qry.Rels[i])

			} else if attr, ok := rel.AttrsMap[name]; ok {
				if flg {
					attr.IncRef()
				}
				if typ == nil {
					typ = &attr.Type
				}
				names = append(names, qry.Rels[i])
			}
		}
	}
	if len(names) == 0 { // scope of query
		for i, e := range qry.ProjectionExtends {
			if e.Alias == name {
				if flg {
					qry.ProjectionExtends[i].IncRef()
				}
				if typ == nil {
					typ = &types.Type{Oid: e.E.ReturnType()}
				}
				if e.Alias == col {
					names = append(names, qry.Name())
				}
			}
		}
	}
	return names, typ, nil
}

// If flg is set, then it will increase the reference count
func (qry *Query) getJoinAttribute(flg bool, tbls []string, col string) (string, string, error) {
	var names []string

	tbl, name := util.SplitTableAndColumn(col)
	if len(tbl) > 0 {
		for i := 0; i < len(tbls); i++ {
			if tbls[i] == tbl {
				attr, ok := qry.RelsMap[tbl].AttrsMap[name]
				if !ok {
					return "", "", errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Column '%s' doesn't exist", col))
				}
				if flg {
					attr.IncRef()
				}
				names = append(names, tbls[i])
			}
		}
		if len(names) == 0 {
			return "", "", errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Table '%s' doesn't exist", tbl))
		}
	} else {
		for i := 0; i < len(tbls); i++ {
			if attr, ok := qry.RelsMap[tbls[i]].AttrsMap[name]; ok {
				if flg {
					attr.IncRef()
				}
				names = append(names, tbls[i])
			}
		}
	}
	if len(names) == 0 {
		return "", "", errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Unknown column '%s' in 'from clause'", col))
	}
	if len(names) > 1 {
		return "", "", errors.New(errno.DuplicateColumn, fmt.Sprintf("Column '%s' in on clause is ambiguous", col))
	}
	return names[0], name, nil
}
