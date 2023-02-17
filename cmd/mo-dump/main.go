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
	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	defaultUsername        = "dump"
	defaultPassword        = "111"
	defaultHost            = "127.0.0.1"
	defaultPort            = 6001
	defaultNetBufferLength = mpool.MB
	minNetBufferLength     = mpool.KB * 16
	maxNetBufferLength     = mpool.MB * 16
	timeout                = 10 * time.Second
)

var (
	conn *sql.DB
)

type Column struct {
	Name string
	Type string
}

type Table struct {
	Name string
	Kind string
}

type Tables []Table

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
		createDb, err = getCreateDB(database)
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
	for i, create := range createTable {
		tbl := tables[i]
		switch tbl.Kind {
		case catalog.SystemOrdinaryRel:
			fmt.Printf("DROP TABLE IF EXISTS `%s`;\n", tbl.Name)
			showCreateTable(create, false)
			err = showInsert(database, tbl.Name, bufPool, netBufferLength)
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
	return tables, nil
}

func getCreateDB(db string) (string, error) {
	r := conn.QueryRow("show create database `" + db + "`")
	var create string
	err := r.Scan(&db, &create)
	if err != nil {
		return "", err
	}
	return create, nil
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

func showInsert(db string, tbl string, bufPool *sync.Pool, netBufferLength int) error {
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

func convertValue(v any, typ string) string {
	ret := *(v.(*sql.RawBytes))
	if ret == nil {
		return "NULL"
	}
	typ = strings.ToLower(typ)
	switch typ {
	case "int", "tinyint", "smallint", "bigint", "unsigned bigint", "unsigned int", "unsigned tinyint", "unsigned smallint", "float", "double", "bool", "boolean", "":
		// why empty string in column type?
		// see https://github.com/matrixorigin/matrixone/issues/8050#issuecomment-1431251524
		return string(ret)
	default:
		return "'" + strings.Replace(string(ret), "'", "\\'", -1) + "'"
	}
}
