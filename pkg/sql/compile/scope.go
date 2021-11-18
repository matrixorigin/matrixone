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

package compile

import (
	"fmt"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/plan"
	"matrixone/pkg/vectorize/like"
	"matrixone/pkg/vm/engine"
	"strings"
)

// CreateDatabase do create database work according to create database plan.
func (s *Scope) CreateDatabase(ts uint64) error {
	p, _ := s.Plan.(*plan.CreateDatabase)
	if _, err := p.E.Database(p.Id); err == nil {
		if p.IfNotExistFlag {
			return nil
		}
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("database %s already exists", p.Id))
	}
	return p.E.Create(ts, p.Id, 0)
}

// CreateTable do create table work according to create table plan.
func (s *Scope) CreateTable(ts uint64) error {
	p, _ := s.Plan.(*plan.CreateTable)
	if r, err := p.Db.Relation(p.Id); err == nil {
		r.Close()
		if p.IfNotExistFlag {
			return nil
		}
		return errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("table '%s' already exists", p.Id))
	}
	return p.Db.Create(ts, p.Id, p.Defs)
}

// CreateIndex do create index work according to create index plan
func (s *Scope) CreateIndex(ts uint64) error {
	return errors.New(errno.FeatureNotSupported, "not support now.")

	//o, _ := s.Plan.(*plan.CreateIndex)
	//defer o.Relation.Close()
	//err := o.Relation.CreateIndex(ts, o.Defs)
	//if o.IfNotExistFlag && err != nil && err.String() == "index already exist" {
	//	return nil
	//}
	//return err
}

// DropDatabase do drop database work according to drop index plan
func (s *Scope) DropDatabase(ts uint64) error {
	p, _ := s.Plan.(*plan.DropDatabase)
	if _, err := p.E.Database(p.Id); err != nil {
		if p.IfExistFlag {
			return nil
		}
		return err
	}
	return p.E.Delete(ts, p.Id)
}

// DropTable do drop table work according to drop table plan
func (s *Scope) DropTable(ts uint64) error {
	p, _ := s.Plan.(*plan.DropTable)
	for i := range p.Dbs {
		db, err := p.E.Database(p.Dbs[i])
		if err != nil {
			if p.IfExistFlag {
				continue
			}
			return err
		}
		if r, err := db.Relation(p.Ids[i]); err != nil {
			if p.IfExistFlag {
				continue
			}
			return err
		} else {
			r.Close()
		}
		if err := db.Delete(ts, p.Ids[i]); err != nil {
			return err
		}
	}
	return nil
}

// DropIndex do drop index word according to drop index plan
func (s *Scope) DropIndex(ts uint64) error {
	return errors.New(errno.FeatureNotSupported, "not support now.")

	//p, _ := s.Plan.(*plan.DropIndex)
	//defer p.Relation.Close()
	//err := p.Relation.DropIndex(ts, p.Id)
	//if p.IfExistFlag && err != nil && err.String == "index not exist" {
	//	return nil
	//}
	//return err
}

// todo: show should get information from system table next day.

// ShowDatabases will show all database names
func (s *Scope) ShowDatabases(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	p, _ := s.Plan.(*plan.ShowDatabases)
	attrs := p.ResultColumns()
	bat := batch.New(true, []string{attrs[0].Name})
	// Column 1
	{
		rs := p.E.Databases()
		vs := make([][]byte, len(rs))

		// like
		count := 0
		if p.Like == nil {
			for _, r := range rs {
				vs[count] = []byte(r)
				count++
			}
		} else {
			tempSlice := make([]int64, 1)
			for _, r := range rs {
				str := []byte(r)
				if k, _ := like.PureLikePure(str, p.Like, tempSlice); k != nil {
					vs[count] = str
					count++
				}
			}
		}
		vs = vs[:count]

		vec := vector.New(attrs[0].Type)
		if err := vector.Append(vec, vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
		bat.InitZsOne(count)
	}
	return fill(u, bat)
}

// ShowTables will show all table names in a database
func (s *Scope) ShowTables(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	p, _ := s.Plan.(*plan.ShowTables)
	attrs := p.ResultColumns()
	bat := batch.New(true, []string{attrs[0].Name})
	// Column 1
	{
		rs := p.Db.Relations()
		vs := make([][]byte, len(rs))

		// like
		count := 0
		if p.Like == nil {
			for _, r := range rs {
				vs[count] = []byte(r)
				count++
			}
		} else {
			tempSlice := make([]int64, 1)
			for _, r := range rs {
				str := []byte(r)
				if k, _ := like.PureLikePure(str, p.Like, tempSlice); k != nil {
					vs[count] = str
					count++
				}
			}
		}
		vs = vs[:count]

		vec := vector.New(attrs[0].Type)
		if err := vector.Append(vec, vs); err != nil {
			return err
		}
		bat.Vecs[0] = vec
		bat.InitZsOne(count)
	}
	return fill(u, bat)
}

type columnInfo struct {
	name 	 string
	typ	 	 types.Type
	dft	  	 string // default value
}

// ShowColumns will show column information from a table
func (s *Scope) ShowColumns(u interface{}, fill func(interface{}, *batch.Batch) error) error {
	p, _ := s.Plan.(*plan.ShowColumns)
	results := p.ResultColumns() // field, type, null, key, default, extra
	defs := p.Relation.TableDefs()
	attrs := make([]columnInfo, len(results))

	names := make([]string, 0)
	for _, resultColumn := range results {
		names = append(names, resultColumn.Name)
	}

	count := 0
	tmpSlice := make([]int64, 1)
	for _, def := range defs {
		if attrDef, ok := def.(*engine.AttributeDef); ok {
			if p.Like != nil {	// deal with like rule
				if k, _ := like.PureLikePure([]byte(attrDef.Attr.Name), p.Like, tmpSlice); k == nil {
					continue
				}
			}
			attrs[count] = columnInfo{
				name:     attrDef.Attr.Name,
				typ:      attrDef.Attr.Type,
			}
			if attrDef.Attr.HasDefaultExpr() {
				attrs[count].dft = fmt.Sprintf("%v", attrDef.Attr.Default.Value)
			}
			count++
		}
	}
	attrs = attrs[:count]

	bat := batch.New(true, names)
	for i := range bat.Vecs {
		bat.Vecs[i] = vector.New(results[i].Type)
	}

	vnames := make([][]byte, len(attrs))
	vtyps := make([][]byte, len(attrs))
	vdfts := make([][]byte, len(attrs))
	undefine := make([][]byte, len(attrs))

	for i, attr := range attrs {
		var typ string

		if attr.typ.Width > 0 {
			typ = fmt.Sprintf("%s(%v)", strings.ToLower(attr.typ.String()), attr.typ.Width)
		} else {
			typ = strings.ToLower(attr.typ.String())
		}

		vnames[i] = []byte(attr.name)
		vtyps[i] = []byte(typ)
		vdfts[i] = []byte(attr.dft)
		undefine[i] = []byte("")
	}

	vector.Append(bat.Vecs[0], vnames) 	 // field
	vector.Append(bat.Vecs[1], vtyps)	 // type
	vector.Append(bat.Vecs[2], undefine) // null todo: not implement
	vector.Append(bat.Vecs[3], undefine) // key todo: not implement
	vector.Append(bat.Vecs[4], vdfts)	 // default
	vector.Append(bat.Vecs[5], undefine) // extra todo: not implement

	bat.InitZsOne(count)
	return fill(u, bat)
}