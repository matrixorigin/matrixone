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
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	_ "github.com/go-sql-driver/mysql"
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

type Options struct {
	username             string
	password             string
	host                 string
	database             string
	tbl                  string
	dbs                  []string
	tables               Tables
	port                 int
	netBufferLength      int
	toCsv                bool
	localInfile          bool
	noData               bool
	emptyTables          bool
	csvConf              csvConfig
	csvFieldDelimiterStr string
}

func (t *Tables) String() string {
	return fmt.Sprint(*t)
}

func (t *Tables) Set(value string) error {
	*t = append(*t, Table{value, ""})
	return nil
}

func main() {
	var (
		err error
		opt Options
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
			if opt.toCsv {
				fmt.Fprintf(os.Stdout, "/* !!!MUST KEEP FILE IN CURRENT DIRECTORY, OR YOU SHOULD CHANGE THE PATH IN LOAD DATA STMT!!! */ \n")
			}
		}
	}()

	ctx := context.Background()
	flag.StringVar(&opt.username, "u", defaultUsername, "username")
	flag.StringVar(&opt.password, "p", defaultPassword, "password")
	flag.StringVar(&opt.host, "h", defaultHost, "hostname")
	flag.IntVar(&opt.port, "P", defaultPort, "portNumber")
	flag.IntVar(&opt.netBufferLength, "net-buffer-length", defaultNetBufferLength, "net_buffer_length")
	flag.StringVar(&opt.database, "db", "", "databaseName, must be specified")
	flag.StringVar(&opt.tbl, "tbl", "", "tableNameList, default all")
	flag.BoolVar(&opt.toCsv, "csv", defaultCsv, "set export format to csv")
	flag.StringVar(&opt.csvFieldDelimiterStr, "csv-field-delimiter", string(defaultFieldDelimiter), "set csv field delimiter (only one utf8 character). enabled only when the option 'csv' is set.")
	flag.BoolVar(&opt.localInfile, "local-infile", defaultLocalInfile, "use load data local infile")
	flag.BoolVar(&opt.noData, "no-data", defaultNoData, "dump database and table definitions only without data")
	flag.Parse()

	if opt.netBufferLength < minNetBufferLength {
		fmt.Fprintf(os.Stderr, "net_buffer_length must be greater than %d, set to %d\n", minNetBufferLength, minNetBufferLength)
		opt.netBufferLength = minNetBufferLength
	}
	if opt.netBufferLength > maxNetBufferLength {
		fmt.Fprintf(os.Stderr, "net_buffer_length must be less than %d, set to %d\n", maxNetBufferLength, maxNetBufferLength)
		opt.netBufferLength = maxNetBufferLength
	}
	opt.dbs = strings.Split(opt.database, ",")
	if len(opt.dbs) == 0 {
		err = moerr.NewInvalidInput(ctx, "database must be specified")
		return
	}
	if len(opt.tbl) > 0 {
		tbls := strings.Split(opt.tbl, ",")
		for _, t := range tbls {
			if len(t) != 0 {
				opt.tables = append(opt.tables, Table{t, ""})
			}
		}
	}

	//replace : in username to #, because : is used as separator in dsn.
	//password can have ":".
	opt.username = strings.ReplaceAll(opt.username, ":", "#")

	// if host has ":", reports error
	if strings.Count(opt.host, ":") > 0 {
		err = moerr.NewInvalidInput(ctx, "host can not have character ':'")
		return
	}

	if opt.toCsv {
		opt.csvConf.enable = opt.toCsv
		opt.csvConf.fieldDelimiter, err = checkFieldDelimiter(ctx, opt.csvFieldDelimiterStr)
		if err != nil {
			return
		}
	}

	if opt.database == "all" {
		conn, err = opt.openDBConnection(ctx, "")
		if err != nil {
			return
		}
		defer conn.Close()

		opt.dbs, err = getDatabases(ctx)
		if err != nil {
			return
		}
		if opt.tables == nil {
			opt.emptyTables = true
		}
	}

	err = opt.dumpData(ctx)
	if err != nil {
		return
	}
}

func (opt *Options) dumpData(ctx context.Context) error {
	var (
		createDb    string
		createTable []string
		err         error
	)

	if conn == nil {
		conn, err = opt.openDBConnection(ctx, opt.dbs[0])
		if err != nil {
			return err
		}
		defer conn.Close()
	}

	for _, db := range opt.dbs {
		if opt.emptyTables {
			opt.tables = nil
		}
		if len(opt.tables) == 0 { //dump all tables
			createDb, err = getCreateDB(ctx, db)
			if err != nil {
				return err
			}
			fmt.Printf("DROP DATABASE IF EXISTS `%s`;\n", db)
			fmt.Println(createDb, ";")
			fmt.Printf("USE `%s`;\n\n\n", db)
		}
		opt.tables, err = getTables(db, opt.tables)
		if err != nil {
			return err
		}
		createTable = make([]string, len(opt.tables))
		for i, tbl := range opt.tables {
			createTable[i], err = getCreateTable(db, tbl.Name)
			if err != nil {
				return err
			}
		}
		bufPool := &sync.Pool{
			New: func() any {
				return &bytes.Buffer{}
			},
		}
		left, right := 0, len(createTable)-1
		for left < right {
			for left < len(createTable) && opt.tables[left].Kind != catalog.SystemViewRel {
				left++
			}
			for right >= 0 && opt.tables[right].Kind == catalog.SystemViewRel {
				right--
			}
			if left >= right {
				break
			}
			createTable[left], createTable[right] = createTable[right], createTable[left]
			opt.tables[left], opt.tables[right] = opt.tables[right], opt.tables[left]
		}
		adjustViewOrder(createTable, opt.tables, left)
		for i, create := range createTable {
			tbl := opt.tables[i]
			switch tbl.Kind {
			case catalog.SystemOrdinaryRel:
				fmt.Printf("DROP TABLE IF EXISTS `%s`;\n", tbl.Name)
				showCreateTable(create, false)
				if !opt.noData {
					err = genOutput(db, tbl.Name, bufPool, opt.netBufferLength, opt.localInfile, &opt.csvConf)
					if err != nil {
						return err
					}
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
				return err
			}
		}
	}
	return nil
}

func (opt *Options) openDBConnection(ctx context.Context, database string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", opt.username, opt.password, opt.host, opt.port, database)

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	ch := make(chan error)
	go func() {
		err := conn.Ping()
		ch <- err
	}()

	select {
	case err = <-ch:
	case <-time.After(timeout):
		return nil, moerr.NewInternalError(ctx, "connect to %s timeout", dsn)
	}
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func adjustViewOrder(createTable []string, tables Tables, start int) {
	viewName := make([]string, 0)
	viewPos := make(map[string]int)
	cnt := len(tables)
	for i := start; i < cnt; i++ {
		viewPos[tables[i].Name] = i - start
		viewName = append(viewName, tables[i].Name)
	}
	viewCount := make([]int, len(viewName))
	viewRef := make([][]int, len(viewName))
	for i := start; i < cnt; i++ {
		for j := start; j < cnt; j++ {
			if i == j {
				continue
			}
			if strings.Count(createTable[i], tables[j].Name) > 0 {
				viewCount[viewPos[tables[i].Name]]++
				viewRef[viewPos[tables[j].Name]] = append(viewRef[viewPos[tables[j].Name]], viewPos[tables[i].Name])
			}
		}
	}
	order := 0
	orderArr := make([]int, 0)
	visit := make([]bool, len(viewName))
	for order < len(viewName) {
		for i := 0; i < len(viewName); i++ {
			if viewCount[i] == 0 && !visit[i] {
				visit[i] = true
				order++
				orderArr = append(orderArr, i)
				for j := 0; j < len(viewRef[i]); j++ {
					viewCount[viewRef[i][j]]--
				}
			}
		}
	}
	newCreate := make([]string, cnt)
	newTables := make([]Table, cnt)
	for i := 0; i < len(orderArr); i++ {
		newCreate[i] = createTable[orderArr[i]+start]
		newTables[i] = tables[orderArr[i]+start]
	}
	_ = copy(createTable[start:], newCreate)
	_ = copy(tables[start:], newTables)
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

func getDatabases(ctx context.Context) ([]string, error) {
	r, err := conn.QueryContext(ctx, "show databases")
	if err != nil {
		return nil, err
	}
	if r.Err() != nil {
		return nil, r.Err()
	}
	dbs := make([]string, 0)

	for r.Next() {
		var dbName string
		err := r.Scan(&dbName)
		if err != nil {
			return nil, err
		}
		dbs = append(dbs, dbName)
	}
	defer r.Close()

	return dbs, nil
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

func showLoad(r *sql.Rows, rowResults []any, cols []*Column, db string, tbl string, localInfile bool, csvConf *csvConfig) error {
	fname := fmt.Sprintf("%s_%s.%s", db, tbl, "csv")
	pwd := os.Getenv("PWD")
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()

	err = toCsv(r, f, rowResults, cols, csvConf)
	if err != nil {
		return err
	}
	if localInfile {
		fmt.Printf("LOAD DATA LOCAL INFILE '%s' INTO TABLE `%s` FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' PARALLEL 'TRUE';\n", fmt.Sprintf("%s/%s", pwd, fname), tbl)
	} else {
		fmt.Printf("LOAD DATA INFILE '%s' INTO TABLE `%s` FIELDS TERMINATED BY '\\t' ENCLOSED BY '\"' LINES TERMINATED BY '\\n' PARALLEL 'TRUE';\n", fmt.Sprintf("%s/%s", pwd, fname), tbl)
	}
	return nil
}

// toCsv converts the result from mo to csv file
func toCsv(r *sql.Rows, output io.Writer, rowResults []any, cols []*Column, csvConf *csvConfig) error {
	var err error
	csvWriter := csv.NewWriter(output)
	csvWriter.Comma = csvConf.fieldDelimiter
	line := make([]string, len(rowResults))

	for r.Next() {
		err = r.Scan(rowResults...)
		if err != nil {
			return err
		}
		err = toCsvLine(csvWriter, rowResults, cols, line)
		if err != nil {
			return err
		}
	}
	return err
}

// toCsvFields converts the result from mo to string
func toCsvFields(rowResults []any, cols []*Column, line []string) {
	for i, v := range rowResults {
		dt, format := convertValue2(v, cols[i].Type)
		str := fmt.Sprintf(format, dt)
		line[i] = str
	}
}

// toCsvLine converts the result from mo to csv single line
func toCsvLine(csvWriter *csv.Writer, rowResults []any, cols []*Column, line []string) error {
	var err error
	toCsvFields(rowResults, cols, line)
	err = csvWriter.Write(line)
	if err != nil {
		return err
	}
	csvWriter.Flush()
	return err
}

func genOutput(db string, tbl string, bufPool *sync.Pool, netBufferLength int, localInfile bool, csvConf *csvConfig) error {
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
	rowResults := make([]any, 0, len(cols))
	for range cols {
		var v sql.RawBytes
		rowResults = append(rowResults, &v)
	}
	if !csvConf.enable {
		return showInsert(r, rowResults, cols, tbl, bufPool, netBufferLength)
	}
	return showLoad(r, rowResults, cols, db, tbl, localInfile, csvConf)
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
	case "vecf32", "vecf64":
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
	case "vecf32", "vecf64":
		return ret, defaultFmt
	default:
		//note: do not use the quoteFmt instead of the standard package csv,
		//it is error-prone.
		return ret, defaultFmt
	}
}

// checkFieldDelimiter checks string is valid utf8 character and returns rune
func checkFieldDelimiter(ctx context.Context, s string) (rune, error) {
	if utf8.ValidString(s) {
		if utf8.RuneCountInString(s) > 1 {
			return rune(0), moerr.NewInvalidInput(ctx, "there are multiple utf8 characters for csv field delimiter. only one utf8 character is allowed")
		}
		runCh, _ := utf8.DecodeRuneInString(s)
		if runCh == utf8.RuneError {
			return rune(0), moerr.NewInvalidInput(ctx, "csv field delimiter is invalid utf8 character")
		}
		return runCh, nil
	} else {
		return rune(0), moerr.NewInvalidInput(ctx, "csv field delimiter is invalid utf8 character")
	}
}
