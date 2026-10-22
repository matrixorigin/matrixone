// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0.

// A small JSON-lines SQL client for the process-owning native recovery driver.
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type request struct {
	DSN   string `json:"dsn"`
	SQL   string `json:"sql"`
	Query bool   `json:"query"`
}

func run(db *sql.DB, q request) ([][]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if !q.Query {
		_, err := db.ExecContext(ctx, q.SQL)
		return nil, err
	}
	r, err := db.QueryContext(ctx, q.SQL)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	cols, err := r.Columns()
	if err != nil {
		return nil, err
	}
	out := make([][]string, 0)
	for r.Next() {
		values := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err = r.Scan(ptrs...); err != nil {
			return nil, err
		}
		line := make([]string, len(cols))
		for i, v := range values {
			line[i] = v.String
			if !v.Valid {
				line[i] = "<NULL>"
			}
		}
		out = append(out, line)
	}
	return out, r.Err()
}

func main() {
	dbs := map[string]*sql.DB{}
	defer func() {
		for _, db := range dbs {
			db.Close()
		}
	}()
	in := bufio.NewScanner(os.Stdin)
	in.Buffer(make([]byte, 4096), 1<<20)
	out := json.NewEncoder(os.Stdout)
	for in.Scan() {
		var q request
		var data [][]string
		err := json.Unmarshal(in.Bytes(), &q)
		if err == nil {
			connection := dbs[q.DSN]
			if connection == nil {
				connection, err = sql.Open("mysql", q.DSN)
				if err == nil {
					connection.SetMaxOpenConns(1)
					dbs[q.DSN] = connection
				}
			}
			if err == nil {
				data, err = run(connection, q)
			}
		}
		message := ""
		if err != nil {
			message = err.Error()
		}
		if err := out.Encode(map[string]any{"rows": data, "error": message}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
	if err := in.Err(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
