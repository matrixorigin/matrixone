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

package main

import (
	"bytes"
	"context"
	"database/sql"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

func (t *Tables) String() string {
	return fmt.Sprint(*t)
}

func (t *Tables) Set(value string) error {
	*t = append(*t, Table{value, ""})
	return nil
}

func main() {
	var (
		username, password, host, database string
		tables                             Tables
		port, netBufferLength              int
		createDb                           string
		createTable                        []string
		err                                error
		toCsv, localInfile                 bool
	)
	dumpStart := time.Now()
	defer func() {
		if err != nil {
			fmt.Fprintf(os.Stderr, "modump error: %v\n", err)
		}
		if conn != nil {
			err := conn.Close()
			if err != nil {
				fmt.Fprintf(os.Stderr, "modump error while close connection: %v\n", err)
			}
		}
		if err == nil {
			fmt.Fprintf(os.Stdout, "/* MODUMP SUCCESS, COST %v */\n", time.Since(dumpStart))
			if toCsv {
				fmt.Fprintf(os.Stdout, "/* !!!MUST KEEP FILE IN CURRENT DIRECTORY, OR YOU SHOULD CHANGE THE PATH IN LOAD DATA STMT!!! */ \n")
			}
		}
	}()

	ctx := context.Background()
	flag.StringVar(&username, "u", defaultUsername, "username")
	flag.StringVar(&password, "p", defaultPassword, "password")
	flag.StringVar(&host, "h", defaultHost, "hostname")
	flag.IntVar(&port, "P", defaultPort, "portNumber")
	flag.IntVar(&netBufferLength, "net-buffer-length", defaultNetBufferLength, "net_buffer_length")
	flag.StringVar(&database, "db", "", "databaseName, must be specified")
	flag.Var(&tables, "tbl", "tableNameList, default all")
	flag.BoolVar(&toCsv, "csv", defaultCsv, "set export format to csv")
	flag.BoolVar(&localInfile, "local-infile", defaultLocalInfile, "use load data local infile")
	flag.Parse()
	if netBufferLength < minNetBufferLength {
		fmt.Fprintf(os.Stderr, "net_buffer_length must be greater than %d, set to %d\n", minNetBufferLength, minNetBufferLength)
		netBufferLength = minNetBufferLength
	}
	if netBufferLength > maxNetBufferLength {
		fmt.Fprintf(os.Stderr, "net_buffer_length must be less than %d, set to %d\n", maxNetBufferLength, maxNetBufferLength)
		netBufferLength = maxNetBufferLength
	}
	if len(database) == 0 {
		err = moerr.NewInvalidInput(ctx, "database must be specified")
		return
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)
	conn, err = sql.Open("mysql", dsn) // Open doesn't open a connection. Validate DSN data:
	if err != nil {
		return
	}
	ch := make(chan error)
	go func() {
		err := conn.Ping() // Before use, we must ping to validate DSN data:
		ch <- err
	}()

	select {
	case err = <-ch:
	case <-time.After(timeout):
		err = moerr.NewInternalError(ctx, "connect to %s timeout", dsn)
	}
	if err != nil {
		return
	}
	if len(tables) == 0 { //dump all tables
		createDb, err = getCreateDB(ctx, database)
		if err != nil {
			return
		}
		fmt.Printf("DROP DATABASE IF EXISTS `%s`;\n", database)
		fmt.Println(createDb, ";")
		fmt.Printf("USE `%s`;\n\n\n", database)
	}
	tables, err = getTables(database, tables)
	if err != nil {
		return
	}
	createTable = make([]string, len(tables))
	for i, tbl := range tables {
		createTable[i], err = getCreateTable(database, tbl.Name)
		if err != nil {
			return
		}
	}
	bufPool := &sync.Pool{
		New: func() any {
			return &bytes.Buffer{}
		},
	}
	left, right := 0, len(createTable)-1
	for left < right {
		for left < len(createTable) && tables[left].Kind != catalog.SystemViewRel {
			left++
		}
		for right >= 0 && tables[right].Kind == catalog.SystemViewRel {
			right--
		}
		if left >= right {
			break
		}
		createTable[left], createTable[right] = createTable[right], createTable[left]
		tables[left], tables[right] = tables[right], tables[left]
	}
	for i, create := range createTable {
		tbl := tables[i]
		switch tbl.Kind {
		case catalog.SystemOrdinaryRel:
			fmt.Printf("DROP TABLE IF EXISTS `%s`;\n", tbl.Name)
			showCreateTable(create, false)
			err = genOutput(database, tbl.Name, bufPool, netBufferLength, toCsv, localInfile)
			if err != nil {
				return
			}
		case catalog.SystemExternalRel:
			fmt.Printf("/*!EXTERNAL TABLE `%s`*/\n", tbl.Name)
			fmt.Printf("DROP TABLE IF EXISTS `%s`;\n", tbl.Name)
			showCreateTable(create, true)
		case catalog.SystemViewRel:
			fmt.Printf("DROP VIEW IF EXISTS `%s`;\n", tbl.Name)
			showCreateTable(create, true)
		default:
			err = moerr.NewNotSupported(ctx, "table type %s", tbl.Kind)
			return
		}
	}
}

func showCreateTable(createSql string, withNextLine bool) {
	var suffix string
	if !strings.HasSuffix(createSql, ";") {
		suffix = ";"
	}
	if withNextLine {
		suffix += "\n\n"
	}
	fmt.Printf("%s%s\n", createSql, suffix)
}

func getTables(db string, tables Tables) (Tables, error) {
	sql := "select relname,relkind from mo_catalog.mo_tables where reldatabase = '" + db + "'"
	if len(tables) > 0 {
		sql += " and relname in ("
		for i, tbl := range tables {
			if i != 0 {
				sql += ","
			}
			sql += "'" + tbl.Name + "'"
		}
		sql += ")"
	}
	r, err := conn.Query(sql) //TODO: after unified sys table prefix, add condition in where clause
	if err != nil {
		return nil, err
	}
	defer r.Close()

	if tables == nil {
		tables = Tables{}
	}
	tables = tables[:0]
	for r.Next() {
		var table string
		var kind string
		err = r.Scan(&table, &kind)
		if err != nil {
			return nil, err
		}
		if strings.HasPrefix(table, "__mo_") || strings.HasPrefix(table, "%!%") { //TODO: after adding condition in where clause, remove this
			continue
		}
		tables = append(tables, Table{table, kind})
	}
	if err := r.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func getCreateDB(ctx context.Context, db string) (string, error) {
	r := conn.QueryRow("show create database `" + db + "`")
	var create string
	err := r.Scan(&db, &create)
	if err != nil {
		return "", err
	}
	// What if it is a subscription database?
	return create, err
}

func getCreateTable(db, tbl string) (string, error) {
	r := conn.QueryRow("show create table `" + db + "`.`" + tbl + "`")
	var create string
	err := r.Scan(&tbl, &create)
	if err != nil {
		return "", err
	}
	return create, nil
}

func showInsert(r *sql.Rows, args []any, cols []*Column, tbl string, bufPool *sync.Pool, netBufferLength int) error {
	var err error
	buf := bufPool.Get().(*bytes.Buffer)
	curBuf := bufPool.Get().(*bytes.Buffer)
	buf.Grow(netBufferLength)
	initInert := "INSERT INTO `" + tbl + "` VALUES "
	for {
		buf.WriteString(initInert)
		preLen := buf.Len()
		first := true
		if curBuf.Len() > 0 {
			bts := curBuf.Bytes()
			if bts[0] == ',' {
				bts = bts[1:]
			}
			buf.Write(bts)
			curBuf.Reset()
			first = false
		}
		for r.Next() {
			err = r.Scan(args...)
			if err != nil {
				return err
			}
			if !first {
				curBuf.WriteString(",(")
			} else {
				curBuf.WriteString("(")
				first = false
			}

			for i, v := range args {
				if i > 0 {
					curBuf.WriteString(",")
				}
				curBuf.WriteString(convertValue(v, cols[i].Type))
			}
			curBuf.WriteString(")")
			if buf.Len()+curBuf.Len() >= netBufferLength {
				break
			}
			buf.Write(curBuf.Bytes())
			curBuf.Reset()
		}
		if buf.Len() > preLen {
			buf.WriteString(";\n")
			_, err = buf.WriteTo(os.Stdout)
			if err != nil {
				return err
			}
			continue
		}
		if curBuf.Len() > 0 {
			continue
		}
		buf.Reset()
		curBuf.Reset()
		break
	}
	bufPool.Put(buf)
	bufPool.Put(curBuf)
	fmt.Printf("\n\n\n")
	return nil
}

func showLoad(r *sql.Rows, args []any, cols []*Column, db string, tbl string, localInfile bool) error {
	fname := fmt.Sprintf("%s_%s.%s", db, tbl, "csv")
	pwd := os.Getenv("PWD")
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	for r.Next() {
		err = r.Scan(args...)
		if err != nil {
			return err
		}
		for i, v := range args {
			dt, format := convertValue2(v, cols[i].Type)
			_, err = fmt.Fprintf(f, format, dt)
			if err != nil {
				return err
			}
			ch := '\t'
			if i == len(args)-1 {
				ch = '\n'
			}
			_, err = fmt.Fprintf(f, "%c", ch)
			if err != nil {
				return err
			}
		}
	}
	if localInfile {
		fmt.Printf("LOAD DATA LOCAL INFILE '%s' INTO TABLE `%s` FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' PARALLEL 'TRUE';\n", fmt.Sprintf("%s/%s", pwd, fname), tbl)
	} else {
		fmt.Printf("LOAD DATA INFILE '%s' INTO TABLE `%s` FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' PARALLEL 'TRUE';\n", fmt.Sprintf("%s/%s", pwd, fname), tbl)
	}
	return nil
}

func genOutput(db string, tbl string, bufPool *sync.Pool, netBufferLength int, toCsv bool, localInfile bool) error {
	r, err := conn.Query("select * from `" + db + "`.`" + tbl + "`")
	if err != nil {
		return err
	}
	colTypes, err := r.ColumnTypes()
	if err != nil {
		return err
	}
	cols := make([]*Column, 0, len(colTypes))
	for _, col := range colTypes {
		var c Column
		c.Name = col.Name()
		c.Type = col.DatabaseTypeName()
		cols = append(cols, &c)
	}
	args := make([]any, 0, len(cols))
	for range cols {
		var v sql.RawBytes
		args = append(args, &v)
	}
	if !toCsv {
		return showInsert(r, args, cols, tbl, bufPool, netBufferLength)
	}
	return showLoad(r, args, cols, db, tbl, localInfile)
}

func convertValue(v any, typ string) string {
	ret := *(v.(*sql.RawBytes))
	if ret == nil {
		return "NULL"
	}
	typ = strings.ToLower(typ)
	switch typ {
	case "float":
		retStr := string(ret)
		if (retStr[0] >= '0' && retStr[0] <= '9') || (retStr[0] == '-' && retStr[1] >= '0' && retStr[1] <= '9') {
			return retStr
		}
		return "'" + retStr + "'" // NaN, +Inf, -Inf, maybe no hacking need in the future
	case "int", "tinyint", "smallint", "bigint", "unsigned bigint", "unsigned int", "unsigned tinyint", "unsigned smallint", "double", "bool", "boolean", "":
		// why empty string in column type?
		// see https://github.com/matrixorigin/matrixone/issues/8050#issuecomment-1431251524
		return string(ret)
	default:
		str := strings.Replace(string(ret), "\\", "\\\\", -1)
		return "'" + strings.Replace(str, "'", "\\'", -1) + "'"
	}
}

func convertValue2(v any, typ string) (sql.RawBytes, string) {
	ret := *(v.(*sql.RawBytes))
	if ret == nil {
		return nullBytes, defaultFmt
	}
	typ = strings.ToLower(typ)
	switch typ {
	case "int", "tinyint", "smallint", "bigint", "unsigned bigint", "unsigned int", "unsigned tinyint", "unsigned smallint", "double", "bool", "boolean", "", "float":
		// why empty string in column type?
		// see https://github.com/matrixorigin/matrixone/issues/8050#issuecomment-1431251524
		return ret, defaultFmt
	case "json":
		return ret, jsonFmt
	default:
		return ret, quoteFmt
	}
}
