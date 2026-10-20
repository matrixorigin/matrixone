// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type WorkerRequest struct {
	Version         int    `json:"version"`
	Operation       string `json:"operation"`
	Engine          Engine `json:"engine"`
	DatasetRoot     string `json:"dataset_root"`
	OutputRoot      string `json:"output_root"`
	Queries         []int  `json:"queries,omitempty"`
	Repetitions     int    `json:"repetitions,omitempty"`
	QueryDeadlineNS int64  `json:"query_deadline_ns"`
	Reference       bool   `json:"reference,omitempty"`
	Unlimited       bool   `json:"unlimited_diagnostic,omitempty"`
}
type QueryReceipt struct {
	Query      int     `json:"query"`
	Repetition int     `json:"repetition"`
	Capture    Capture `json:"capture"`
	Error      string  `json:"error,omitempty"`
	Reference  string  `json:"reference,omitempty"`
}
type WorkerReceipt struct {
	Version       int               `json:"version"`
	Engine        string            `json:"engine"`
	EngineVersion string            `json:"engine_version"`
	GoVersion     string            `json:"go_version"`
	Build         string            `json:"build"`
	Dataset       FileID            `json:"dataset"`
	Binary        FileID            `json:"binary"`
	Settings      map[string]string `json:"effective_settings"`
	Variant       string            `json:"variant"`
	Started       time.Time         `json:"started"`
	Finished      time.Time         `json:"finished"`
	Queries       []QueryReceipt    `json:"queries,omitempty"`
	Load          *LoadReceipt      `json:"load,omitempty"`
	Error         string            `json:"error,omitempty"`
	Cleanup       string            `json:"cleanup"`
}

var identifier = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

func createMODatabase(ctx context.Context, e Engine) error {
	if !identifier.MatchString(e.Database) || !strings.HasPrefix(e.Database, "tpch_compare_") {
		return errors.New("invalid private database name")
	}
	cfg, err := mysql.ParseDSN(os.Getenv(e.DSNEnv))
	if err != nil {
		return errors.New("invalid MO DSN")
	}
	if cfg.Net != "tcp" || !strings.HasPrefix(cfg.Addr, "127.0.0.1:") {
		return errors.New("owned MO deployment must use numeric loopback")
	}
	cfg.DBName = ""
	cfg.Timeout = 10 * time.Second
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.ExecContext(ctx, "CREATE DATABASE "+e.Database)
	return err
}

func runWorker(ctx context.Context, r WorkerRequest) (out WorkerReceipt, retErr error) {
	out = WorkerReceipt{Version: formatVersion, Engine: r.Engine.Name, GoVersion: runtime.Version(), Started: time.Now().UTC(), Cleanup: "failed"}
	out.Variant = "qualification"
	if r.Unlimited {
		out.Variant = "unlimited_diagnostic"
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		out.Build = info.String()
	}
	defer func() {
		out.Finished = time.Now().UTC()
		if retErr != nil {
			out.Error = retErr.Error()
		}
	}()
	if r.Version != formatVersion || r.Engine.Threads < 1 || r.Engine.Threads > 64 || r.Engine.MemoryBytes < 128<<20 || r.Repetitions < 0 || r.Repetitions > 2000 || r.QueryDeadlineNS <= 0 {
		return out, errors.New("invalid bounded worker request")
	}
	if err := os.Mkdir(r.OutputRoot, 0750); err != nil {
		return out, fmt.Errorf("worker output must be a new directory: %w", err)
	}
	if err := publishJSON(filepath.Join(r.OutputRoot, "owner.json"), map[string]any{"purpose": "tpch-compare worker", "version": formatVersion}); err != nil {
		return out, err
	}
	d, err := loadDataset(r.DatasetRoot, r.Operation == "verify")
	if err != nil {
		return out, err
	}
	out.Dataset, err = identify(filepath.Join(r.DatasetRoot, "dataset.json"))
	if err != nil {
		return out, err
	}
	exe, err := os.Executable()
	if err != nil {
		return out, err
	}
	out.Binary, err = identify(exe)
	if err != nil {
		return out, err
	}
	if r.Operation == "load" && r.Engine.Name == "mo" {
		if err := createMODatabase(ctx, r.Engine); err != nil {
			return out, err
		}
	}
	db, err := openEngine(ctx, r.Engine)
	if err != nil {
		return out, err
	}
	defer func() {
		err := db.Close()
		retErr = errors.Join(retErr, err)
		if err == nil {
			out.Cleanup = "complete"
		}
	}()
	conn, err := configureConn(ctx, db, r.Engine)
	if err != nil {
		return out, err
	}
	defer func() { retErr = errors.Join(retErr, conn.Close()) }()
	if err := conn.QueryRowContext(ctx, "SELECT version()").Scan(&out.EngineVersion); err != nil {
		return out, err
	}
	out.Settings, err = effectiveSettings(ctx, conn, r.Engine.Name)
	if err != nil {
		return out, err
	}
	switch r.Operation {
	case "load":
		receipt, err := bulkLoad(ctx, conn, r.Engine, r.DatasetRoot, d)
		out.Load = &receipt
		return out, err
	case "verify":
		for _, t := range d.Tables {
			if err := verifyTableSchema(ctx, conn, r.Engine.Name, t); err != nil {
				return out, err
			}
			if err := verifyTable(ctx, conn, r.DatasetRoot, t, d.Inputs[t.Name]); err != nil {
				return out, err
			}
		}
		return out, verifyRelationships(ctx, conn)
	case "queries":
		if r.Repetitions < 1 || len(r.Queries) < 1 || len(r.Queries) > 22 {
			return out, errors.New("nonempty bounded query selection required")
		}
		for rep := 0; rep < r.Repetitions; rep++ {
			for _, n := range r.Queries {
				if n < 1 || n > 22 {
					return out, errors.New("query outside Q1-Q22")
				}
				q := d.Queries[n-1].rendered(fmt.Sprintf("%d_%d_%d", os.Getpid(), rep, n))
				if r.Unlimited && q.Limit > 0 {
					q.Statements[q.ResultStatement] = strings.TrimSuffix(q.Statements[q.ResultStatement], fmt.Sprintf(" LIMIT %d", q.Limit))
					q.Limit = 0
				}
				queryCtx, cancel := context.WithTimeoutCause(ctx, time.Duration(r.QueryDeadlineNS), fmt.Errorf("query Q%d execution deadline", n))
				cap, err := executeUnit(queryCtx, conn, q, filepath.Join(r.OutputRoot, fmt.Sprintf("q%02d-%04d.rows", n, rep)), captureDisk)
				cancel()
				receipt := QueryReceipt{Query: n, Repetition: rep, Capture: cap}
				if err != nil {
					receipt.Error = err.Error()
					out.Queries = append(out.Queries, receipt)
					return out, err
				}
				if r.Reference {
					err = qualifyReference(r.DatasetRoot, d, q, r.OutputRoot, cap)
					receipt.Reference = "pass"
					if err != nil {
						receipt.Reference = "failed"
						receipt.Error = err.Error()
					}
				}
				out.Queries = append(out.Queries, receipt)
				if err != nil {
					return out, err
				}
			}
		}
		return out, nil
	default:
		return out, fmt.Errorf("unsupported worker operation %q", r.Operation)
	}
}
