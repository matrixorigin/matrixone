// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

func CDCParsePitrGranularity(
	ctx context.Context,
	level string,
	tables string,
) (pts *PatternTuples, err error) {
	pts = &PatternTuples{}

	if level == CDCPitrGranularity_Account {
		pts.Append(&PatternTuple{
			Source: PatternTable{
				Database: CDCPitrGranularity_All,
				Table:    CDCPitrGranularity_All,
			},
			Sink: PatternTable{
				Database: CDCPitrGranularity_All,
				Table:    CDCPitrGranularity_All,
			},
		})
		return
	}

	// split tables by ',' => table pair
	var pt *PatternTuple
	tablePairs := strings.Split(strings.TrimSpace(tables), ",")
	dup := make(map[string]struct{})
	for _, pair := range tablePairs {
		if pt, err = CDCParseGranularityTuple(
			ctx, level, pair, dup,
		); err != nil {
			return
		}
		pts.Append(pt)
	}
	return
}

func CDCParseGranularityTuple(
	ctx context.Context,
	level string,
	pattern string,
	dup map[string]struct{},
) (pt *PatternTuple, err error) {
	splitRes := strings.Split(strings.TrimSpace(pattern), ":")
	if len(splitRes) > 2 {
		err = moerr.NewInternalErrorf(
			ctx, "invalid pattern format: %s, must be `source` or `source:sink`.", pattern,
		)
		return
	}

	pt = &PatternTuple{OriginString: pattern}

	// handle source part
	if pt.Source.Database, pt.Source.Table, err = CDCParseTableInfo(
		ctx, splitRes[0], level,
	); err != nil {
		return
	}
	key := GenDbTblKey(pt.Source.Database, pt.Source.Table)
	if _, ok := dup[key]; ok {
		err = moerr.NewInternalErrorf(
			ctx, "one db/table: %s can't be used as multi sources in a cdc task", key,
		)
		return
	}
	dup[key] = struct{}{}

	// handle sink part
	if len(splitRes) > 1 {
		if pt.Sink.Database, pt.Sink.Table, err = CDCParseTableInfo(
			ctx, splitRes[1], level,
		); err != nil {
			return
		}
	} else {
		// if not specify sink, then sink = source
		pt.Sink.Database = pt.Source.Database
		pt.Sink.Table = pt.Source.Table
	}
	return
}

func CDCParseTableInfo(
	ctx context.Context,
	input string,
	level string,
) (db string, table string, err error) {
	parts := strings.Split(strings.TrimSpace(input), ".")
	if level == CDCPitrGranularity_DB && len(parts) != 1 {
		err = moerr.NewInternalErrorf(ctx, "invalid databases format: %s", input)
		return
	} else if level == CDCPitrGranularity_Table && len(parts) != 2 {
		err = moerr.NewInternalErrorf(ctx, "invalid tables format: %s", input)
		return
	}

	db = strings.TrimSpace(parts[0])
	if !dbNameIsLegal(db) {
		err = moerr.NewInternalErrorf(ctx, "invalid database name: %s", db)
		return
	}

	if level == CDCPitrGranularity_Table {
		table = strings.TrimSpace(parts[1])
		if !tableNameIsLegal(table) {
			err = moerr.NewInternalErrorf(ctx, "invalid table name: %s", table)
			return
		}
	} else {
		table = CDCPitrGranularity_All
	}
	return
}

func dbNameIsLegal(name string) bool {
	name = strings.TrimSpace(name)
	if hasSpecialChars(name) {
		return false
	}
	if name == CDCPitrGranularity_All {
		return true
	}

	createDBSqls := []string{
		"create database " + name,
		"create database `" + name + "`",
	}
	return isLegal(name, createDBSqls)
}

func tableNameIsLegal(name string) bool {
	name = strings.TrimSpace(name)
	if hasSpecialChars(name) {
		return false
	}
	if name == CDCPitrGranularity_All {
		return true
	}

	createTableSqls := []string{
		"create table " + name + "(a int)",
		"create table `" + name + "`(a int)",
	}
	return isLegal(name, createTableSqls)
}

func hasSpecialChars(s string) bool {
	return strings.ContainsAny(s, ",.:`")
}

func isLegal(name string, sqls []string) bool {
	name = strings.TrimSpace(name)
	if len(name) == 0 || len(sqls) == 0 {
		return false
	}
	for _, sql := range sqls {
		if len(sql) == 0 {
			return false
		}
	}
	yes := false
	for _, sql := range sqls {
		_, err := parsers.ParseOne(context.Background(), dialect.MYSQL, sql, 1)
		if err != nil {
			continue
		}
		yes = true
		break
	}
	return yes
}
