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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type limitedWriter struct {
	Writer    io.Writer
	Remaining int64
}

func (w *limitedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.Remaining {
		return 0, errors.New("artifact size limit exceeded")
	}
	n, err := w.Writer.Write(p)
	w.Remaining -= int64(n)
	return n, err
}

func commandOutput(ctx context.Context, dir string, env []string, argv ...string) ([]byte, error) {
	if len(argv) == 0 {
		return nil, errors.New("empty command")
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	cmd.WaitDelay = 5 * time.Second
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &limitedWriter{&stdout, 1 << 20}
	cmd.Stderr = &limitedWriter{&stderr, 1 << 20}
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("%s: %w: %s", filepath.Base(argv[0]), err, stderr.String())
	}
	return stdout.Bytes(), nil
}

// splitSQL handles semicolons in literals/comments. Benchmark SQL has no procedures.
func splitSQL(input string) ([]string, error) {
	var out []string
	var b strings.Builder
	quote := byte(0)
	line, block := false, false
	for i := 0; i < len(input); i++ {
		c := input[i]
		if line {
			if c == '\n' {
				line = false
				b.WriteByte(' ')
			}
			continue
		}
		if block {
			if c == '*' && i+1 < len(input) && input[i+1] == '/' {
				block = false
				i++
				b.WriteByte(' ')
			}
			continue
		}
		if quote != 0 {
			b.WriteByte(c)
			if c == quote {
				if i+1 < len(input) && input[i+1] == quote {
					i++
					b.WriteByte(quote)
				} else {
					quote = 0
				}
			}
			continue
		}
		if c == '-' && i+1 < len(input) && input[i+1] == '-' {
			line = true
			i++
			continue
		}
		if c == '/' && i+1 < len(input) && input[i+1] == '*' {
			block = true
			i++
			continue
		}
		if c == '\'' || c == '"' || c == '`' {
			quote = c
			b.WriteByte(c)
			continue
		}
		if c == ';' {
			if s := strings.TrimSpace(b.String()); s != "" {
				out = append(out, s)
			}
			b.Reset()
			continue
		}
		b.WriteByte(c)
	}
	if quote != 0 || block {
		return nil, errors.New("unterminated SQL literal/comment")
	}
	if s := strings.TrimSpace(b.String()); s != "" {
		out = append(out, s)
	}
	return out, nil
}

var intervalPrecision = regexp.MustCompile(`(?i)(interval\s+'[0-9]+'\s+day)\s*\([0-9]+\)`)
var limitDirective = regexp.MustCompile(`(?i)^limit\s+(-?[0-9]+)$`)

func renderQGen(n int, raw string) (Query, error) {
	if n < 1 || n > 22 {
		return Query{}, errors.New("query outside Q1-Q22")
	}
	q := queryContract(n)
	parts, err := splitSQL(raw)
	if err != nil {
		return q, err
	}
	for _, s := range parts {
		if m := limitDirective.FindStringSubmatch(s); m != nil {
			limit, _ := strconv.Atoi(m[1])
			if limit > 0 {
				if len(q.Statements) == 0 || limit != q.Limit {
					return q, fmt.Errorf("query Q%d unexpected limit directive", n)
				}
				q.Statements[len(q.Statements)-1] += fmt.Sprintf(" LIMIT %d", limit)
			}
			continue
		}
		s = intervalPrecision.ReplaceAllString(s, "$1")
		if n == 15 {
			s = viewName.ReplaceAllString(s, "{{view}}")
		}
		q.Statements = append(q.Statements, s)
	}
	if n == 15 {
		if len(q.Statements) != 3 || !strings.HasPrefix(strings.ToLower(q.Statements[0]), "create view {{view}}") || !strings.HasPrefix(strings.ToLower(q.Statements[2]), "drop view {{view}}") {
			return q, errors.New("Q15 must create, select and drop its private view")
		}
		q.ResultStatement = 1
		q.Cleanup = "DROP VIEW IF EXISTS {{view}}"
	} else if len(q.Statements) != 1 || !strings.HasPrefix(strings.ToLower(q.Statements[0]), "select") {
		return q, fmt.Errorf("query Q%d must contain one SELECT", n)
	}
	if q.Limit > 0 && !strings.HasSuffix(strings.ToUpper(q.Statements[q.ResultStatement]), fmt.Sprintf("LIMIT %d", q.Limit)) {
		return q, fmt.Errorf("query Q%d missing required outer LIMIT", n)
	}
	return q, nil
}

type PrepareOptions struct {
	Root, Tools, Scale, Revision, GeneratorVersion string
	Generate                                       bool
}

func prepare(ctx context.Context, o PrepareOptions) (Dataset, error) {
	var d Dataset
	scale, err := strconv.ParseFloat(o.Scale, 64)
	if err != nil || math.IsNaN(scale) || math.IsInf(scale, 0) || scale <= 0 || scale > 1000 {
		return d, errors.New("scale must be finite, positive, and <= 1000")
	}
	if o.Revision == "" || o.GeneratorVersion == "" {
		return d, errors.New("generator source revision and actual tool version are required")
	}
	root, err := filepath.Abs(o.Root)
	if err != nil {
		return d, err
	}
	tools, err := filepath.Abs(o.Tools)
	if err != nil {
		return d, err
	}
	// A fresh directory is the ownership boundary. Existing input can only be
	// adopted by explicitly placing it in a fresh root before --generate=false.
	if err := os.MkdirAll(filepath.Dir(root), 0750); err != nil {
		return d, err
	}
	if err := os.Mkdir(root, 0750); err != nil {
		return d, fmt.Errorf("prepare requires a new work directory: %w", err)
	}
	if err := publishJSON(filepath.Join(root, "owner.json"), map[string]any{"version": formatVersion, "purpose": "tpch-compare dataset"}); err != nil {
		return d, err
	}
	d = Dataset{Version: formatVersion, Specification: "3.0.1", Scale: o.Scale, Inputs: map[string]FileID{}, Tables: canonicalTables(), References: map[string]FileID{}, Generator: Generator{SourceRevision: o.Revision, Version: o.GeneratorVersion}}
	for name, target := range map[string]*FileID{"dbgen": &d.Generator.DBGen, "qgen": &d.Generator.QGen, "dists.dss": &d.Generator.Distribution} {
		id, err := identify(filepath.Join(tools, name))
		if err != nil {
			return d, err
		}
		*target = id
	}
	env := []string{"DSS_CONFIG=" + tools, "DSS_QUERY=" + filepath.Join(tools, "queries"), "DSS_PATH=" + root, "LC_ALL=C", "TZ=UTC"}
	if !o.Generate {
		return d, errors.New("generation is required; arbitrary existing data is not a qualified dataset")
	}
	if _, err := commandOutput(ctx, root, env, filepath.Join(tools, "dbgen"), "-s", o.Scale); err != nil {
		return d, err
	}
	for _, t := range d.Tables {
		id, err := identify(filepath.Join(root, t.Name+".tbl"))
		if err != nil {
			return d, err
		}
		id.Path = t.Name + ".tbl"
		d.Inputs[t.Name] = id
	}
	for n := 1; n <= 22; n++ {
		tpl, err := identify(filepath.Join(tools, "queries", fmt.Sprintf("%d.sql", n)))
		if err != nil {
			return d, err
		}
		d.Generator.Templates = append(d.Generator.Templates, tpl)
		raw, err := commandOutput(ctx, root, env, filepath.Join(tools, "qgen"), "-d", "-s", o.Scale, strconv.Itoa(n))
		if err != nil {
			return d, err
		}
		q, err := renderQGen(n, string(raw))
		if err != nil {
			return d, err
		}
		name := fmt.Sprintf("q%02d.qgen.sql", n)
		if err := os.WriteFile(filepath.Join(root, name), raw, 0600); err != nil {
			return d, err
		}
		q.Source, err = identify(filepath.Join(root, name))
		if err != nil {
			return d, err
		}
		q.Source.Path = name
		d.Queries = append(d.Queries, q)
		if scale == 1 {
			name := fmt.Sprintf("q%d.out", n)
			source := filepath.Join(tools, "answers", name)
			if err := copyFile(source, filepath.Join(root, name), 16<<20); err != nil {
				return d, fmt.Errorf("SF1 reference Q%d: %w", n, err)
			}
			id, err := identify(filepath.Join(root, name))
			if err != nil {
				return d, err
			}
			id.Path = name
			d.References[strconv.Itoa(n)] = id
		}
	}
	return d, publishJSON(filepath.Join(root, "dataset.json"), d)
}

func copyFile(src, dst string, limit int64) (retErr error) {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		retErr = errors.Join(retErr, out.Close())
		if retErr != nil {
			_ = os.Remove(dst)
		}
	}()
	n, err := io.Copy(out, io.LimitReader(in, limit+1))
	if err != nil {
		return err
	}
	if n > limit {
		return errors.New("copy exceeds artifact bound")
	}
	return out.Sync()
}
