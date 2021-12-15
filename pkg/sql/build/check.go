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

package build

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
)

func (b *build) checkProduct(r, s op.OP) error {
	switch {
	case len(r.Name()) == 0 && len(s.Name()) == 0:
		rattrs, sattrs := r.Attribute(), s.Attribute()
		for attr, _ := range rattrs {
			if _, ok := sattrs[attr]; ok {
				return sqlerror.New(errno.AmbiguousColumn, fmt.Sprintf("conflict attribute '%s'", attr))
			}
		}
	case len(r.Name()) != 0 && len(s.Name()) == 0:
		name := r.Name()
		rattrs, sattrs := r.Attribute(), s.Attribute()
		for attr, _ := range rattrs {
			if _, ok := sattrs[name+"."+attr]; ok {
				return sqlerror.New(errno.AmbiguousColumn, fmt.Sprintf("conflict attribute '%s'", name+"."+attr))
			}
		}
	case len(r.Name()) == 0 && len(s.Name()) != 0:
		name := s.Name()
		rattrs, sattrs := r.Attribute(), s.Attribute()
		for attr, _ := range sattrs {
			if _, ok := rattrs[name+"."+attr]; ok {
				return sqlerror.New(errno.AmbiguousColumn, fmt.Sprintf("conflict attribute '%s'", name+"."+attr))
			}
		}
	}
	return nil
}

func (b *build) checkNaturalJoin(r, s op.OP) error {
	rattrs, sattrs := r.Attribute(), s.Attribute()
	for attr, _ := range rattrs {
		if _, ok := sattrs[attr]; ok {
			return nil
		}
	}
	return sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("no public attributes"))
}

func (b *build) checkInnerJoin(r, s op.OP, rattrs, sattrs []string, expr tree.Expr) ([]string, []string, error) {
	var err error

	switch e := expr.(type) {
	case *tree.AndExpr:
		if rattrs, sattrs, err = b.checkInnerJoin(r, s, rattrs, sattrs, e.Left); err != nil {
			return nil, nil, err
		}
		return b.checkInnerJoin(r, s, rattrs, sattrs, e.Right)
	case *tree.ComparisonExpr:
		var ltyp, rtyp types.Type

		left, ok := e.Left.(*tree.UnresolvedName)
		if !ok {
			return nil, nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition %#v", expr))
		}
		right, ok := e.Right.(*tree.UnresolvedName)
		if !ok {
			return nil, nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition %#v", expr))
		}
		{
			var err error
			var rname, sname string

			if rname, sname, ltyp, err = getJoinAttribute(r, s, left); err != nil {
				return nil, nil, err
			}
			switch {
			case len(rname) > 0:
				rattrs = append(rattrs, rname)
			case len(sname) > 0:
				sattrs = append(sattrs, sname)
			}
		}
		{
			var err error
			var rname, sname string

			if rname, sname, rtyp, err = getJoinAttribute(r, s, right); err != nil {
				return nil, nil, err
			}
			switch {
			case len(rname) > 0:
				rattrs = append(rattrs, rname)
			case len(sname) > 0:
				sattrs = append(sattrs, sname)
			}
		}
		if ltyp.Oid != rtyp.Oid {
			return nil, nil, sqlerror.New(errno.DatatypeMismatch, fmt.Sprintf("'%s' and '%s' type mismatch", left.Parts[0], right.Parts[0]))
		}
		return rattrs, sattrs, nil
	}
	return nil, nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport join condition %#v", expr))

}

func getJoinAttribute(r, s op.OP, name *tree.UnresolvedName) (string, string, types.Type, error) {
	if len(name.Parts[1]) == 0 {
		return "", "", types.Type{}, sqlerror.New(errno.AmbiguousColumn, fmt.Sprintf("column '%s' in on clause is ambiguous", name.Parts[0]))
	}
	rname, sname := r.Name(), s.Name()
	rattrs, sattrs := r.Attribute(), s.Attribute()
	if len(rname) > 0 && rname == name.Parts[1] {
		typ, ok := rattrs[name.Parts[0]]
		if !ok {
			return "", "", types.Type{}, sqlerror.New(errno.UndefinedColumn, fmt.Sprintf("unknown column '%s.%s' in on clause", name.Parts[1], name.Parts[0]))
		}
		return name.Parts[0], "", typ, nil
	}
	if len(sname) > 0 && sname == name.Parts[1] {
		typ, ok := sattrs[name.Parts[0]]
		if !ok {
			return "", "", types.Type{}, sqlerror.New(errno.UndefinedColumn, fmt.Sprintf("unknown column '%s.%s' in on clause", name.Parts[1], name.Parts[0]))
		}
		return "", name.Parts[0], typ, nil
	}
	colName := name.Parts[1] + "." + name.Parts[0]
	if typ, ok := rattrs[colName]; ok {
		return colName, "", typ, nil
	}
	if typ, ok := sattrs[colName]; ok {
		return "", colName, typ, nil
	}
	return "", "", types.Type{}, sqlerror.New(errno.UndefinedColumn, fmt.Sprintf("unknown column '%s' in on clause", colName))
}
