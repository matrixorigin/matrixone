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
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/plan"
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