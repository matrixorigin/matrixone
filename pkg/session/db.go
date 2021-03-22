// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"matrixbase/pkg/errno"
	"matrixbase/pkg/parser"
	"matrixbase/pkg/parser/ast"
	"matrixbase/pkg/sessionctx"
	"matrixbase/pkg/util/dbterror"
)

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx sessionctx.Context, src string) ([]ast.StmtNode, error) {
	// logutil.BgLogger().Debug("compiling", zap.String("source", src))
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.SetParserConfig(ctx.GetSessionVars().BuildParserConfig())
	p.SetSQLMode(ctx.GetSessionVars().SQLMode)
	stmts, warns, err := p.Parse(src, charset, collation)
	for _, warn := range warns {
		ctx.GetSessionVars().StmtCtx.AppendWarning(warn)
	}
	if err != nil {
		return nil, err
	}
	return stmts, nil
}

// Session errors.
var (
	ErrForUpdateCantRetry = dbterror.ClassSession.NewStd(errno.ErrForUpdateCantRetry)
)
