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
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"strconv"
	"strings"
)

const (
	_username = "dump"
	_password = "111"
	_host     = "127.0.0.1"
	_port     = 6001
	batchSize = 4096
)

var (
	conn *sql.DB
)

type Column struct {
	Name string
	Type string
}
type Tables []string

func (t *Tables) String() string {
	return fmt.Sprint(*t)
}

func (t *Tables) Set(value string) error {
	*t = append(*t, value)
	return nil
}

func main() {
	var (
		username, password, host, database string
		tables                             Tables
		port                               int
		createDb                           string
		createTable                        []string
		err                                error
	)
	defer func() {
		if err != nil {
			fmt.Printf("error: %v", err)
		}
		if conn != nil {
			conn.Close()
		}
	}()
	flag.StringVar(&username, "u", _username, "username")
	flag.StringVar(&password, "p", _password, "password")
	flag.StringVar(&host, "h", _host, "hostname")
	flag.IntVar(&port, "P", _port, "portNumber")
	flag.StringVar(&database, "db", "", "databaseName, must be specified")
	flag.Var(&tables, "tbl", "tableNameList, default all")
	flag.Parse()
	if len(database) == 0 {
		err = moerr.NewInvalidInput("database must be specified")
		return
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", username, password, host, port, database)
	conn, err = sql.Open("mysql", dsn) // Open doesn't open a connection. Validate DSN data:
	if err != nil {
		return
	}
	err = conn.Ping() // Before use, we must ping to validate DSN data:
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
		tables, err = getTables()
		if err != nil {
			return
		}
	}
	createTable = make([]string, len(tables))
	for i, tbl := range tables {
		createTable[i], err = getCreateTable(tbl)
		if err != nil {
			return
		}
	}

	for i, create := range createTable {
		tbl := tables[i]
		if isView(create) {
			fmt.Printf("DROP VIEW IF EXISTS `%s`;\n", tbl)
			fmt.Printf("%s;\n\n\n", create)
			continue
		}
		fmt.Printf("DROP TABLE IF EXISTS `%s`;\n", tbl)
		var suffix string
		if !strings.HasSuffix(create, ";") {
			suffix = ";"
		}
		fmt.Printf("%s%s\n", create, suffix)
		err = showInsert(database, tbl)
		if err != nil {
			return
		}
	}
}

func getTables() ([]string, error) {
	r, err := conn.Query("show tables")
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var tables []string
	for r.Next() {
		var table string
		err = r.Scan(&table)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func getCreateDB(db string) (string, error) {
	r := conn.QueryRow("show create database " + db)
	var (
		create string
	)
	err := r.Scan(&db, &create)
	if err != nil {
		return "", err
	}
	return create, nil
}

func getCreateTable(tbl string) (string, error) {
	r := conn.QueryRow("show create table " + tbl)
	var create string
	err := r.Scan(&tbl, &create)
	if err != nil {
		return "", err
	}
	return create, nil
}

func showInsert(db string, tbl string) error {
	r, err := conn.Query("select * from " + db + "." + tbl + " limit 0, " + strconv.Itoa(batchSize))
	if err != nil {
		return err
	}
	cur := 0
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
	buf := new(bytes.Buffer)
	for {
		if !r.Next() {
			break
		}
		buf.WriteString("INSERT INTO `" + tbl + "` VALUES ")
		preLen := buf.Len()
		first := true
		for {
			args := make([]interface{}, 0, len(cols))
			for range cols {
				var v interface{}
				args = append(args, &v)
			}
			err = r.Scan(args...)
			if err != nil {
				return err
			}
			values := "("
			if !first {
				values = ",("
			}
			first = false
			for i, v := range args {
				if i > 0 {
					values += ","
				}
				values += convertValue(v, cols[i].Type)
			}
			values += ")"
			buf.WriteString(values)
			if !r.Next() {
				break
			}
		}
		if buf.Len() > preLen {
			buf.WriteString(";\n")
			fmt.Print(buf.String())
		}

		r.Close()
		buf.Reset()
		cur += batchSize
		r, err = conn.Query("select * from " + db + "." + tbl + " limit " + strconv.Itoa(cur) + ", " + strconv.Itoa(batchSize))
		if err != nil {
			return err
		}
	}
	fmt.Printf("\n\n\n")
	return nil
}

func convertValue(v interface{}, typ string) string {
	typ = strings.ToLower(typ)
	ret := *(v.(*interface{}))
	switch typ {
	case "int", "tinyint", "smallint", "bigint":
		tmp, _ := strconv.ParseInt(string(ret.([]byte)), 10, 64)
		return fmt.Sprintf("%v", tmp)
	case "unsigned bigint", "unsigned int", "unsigned tinyint", "unsigned smallint":
		tmp, _ := strconv.ParseUint(string(ret.([]byte)), 10, 64)
		return fmt.Sprintf("%v", tmp)
	case "float", "double":
		tmp, _ := strconv.ParseFloat(string(ret.([]byte)), 64)
		return fmt.Sprintf("%v", tmp)
	default:
		return fmt.Sprintf("'%v'", string(ret.([]byte)))
	}
}
func isView(sql string) bool {
	sql = strings.ToLower(sql)
	return strings.HasPrefix(sql, "create view")
}
