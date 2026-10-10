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

// mo-sirius-campaign is the public MySQL consumer for parity protocol v2.
package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/go-sql-driver/mysql"
)

const maxRowBytes = 1 << 20

type runSpec struct {
	Sequence      int    `json:"sequence"`
	Route         string `json:"route"`
	Query         int    `json:"query"`
	QuerySHA256   string `json:"query_sha256"`
	ScaleFactor   int    `json:"scale_factor"`
	DatasetSHA256 string `json:"dataset_sha256"`
	Streams       int    `json:"streams"`
	SpoolBudget   int64  `json:"spool_budget_bytes"`
}

type configuration struct {
	DSN            string            `json:"dsn"`
	QueryDirectory string            `json:"query_directory"`
	Scratch        string            `json:"scratch"`
	Databases      map[string]string `json:"databases"`
	Session        []string          `json:"session"`
	TimeoutSeconds int               `json:"timeout_seconds"`
}

type publicColumn struct {
	Name          string `json:"name"`
	SQLType       string `json:"sql_type"`
	Nullable      bool   `json:"nullable"`
	NullableKnown bool   `json:"nullable_known"`
	Precision     int64  `json:"precision"`
	Scale         int64  `json:"scale"`
	DecimalKnown  bool   `json:"decimal_known"`
}

type cell struct {
	Type  string `json:"type"`
	Value string `json:"value"`
	Scale int64  `json:"scale,omitempty"`
	Null  bool   `json:"null,omitempty"`
}

type resultDescriptor struct {
	Schema []publicColumn `json:"public_schema"`
	Rows   uint64         `json:"row_count"`
	Spool  string         `json:"spool"`
	Bytes  int64          `json:"bytes"`
	SHA256 string         `json:"sha256"`
}

type output struct {
	SchemaVersion int              `json:"schema_version"`
	Sequence      int              `json:"sequence"`
	Route         string           `json:"route"`
	QueryID       string           `json:"query_id"`
	WallSeconds   float64          `json:"wall_seconds"`
	FirstRow      *float64         `json:"first_row_seconds"`
	Result        resultDescriptor `json:"result"`
}

func privateFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > maxRowBytes {
		return errors.New("configuration is not a bounded private regular file")
	}
	return nil
}

func readConfiguration(path string) (configuration, error) {
	var cfg configuration
	if err := privateFile(path); err != nil {
		return cfg, err
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	err = json.Unmarshal(data, &cfg)
	if err == nil && (cfg.DSN == "" || cfg.Scratch == "" || cfg.QueryDirectory == "" || cfg.TimeoutSeconds <= 0) {
		err = errors.New("incomplete campaign configuration")
	}
	return cfg, err
}

func exactDecimal(value string, precision, scale int64) (string, error) {
	if precision < 1 || precision > 76 || scale < 0 || scale > precision || len(value) > 160 {
		return "", errors.New("invalid decimal descriptor or value")
	}
	negative := strings.HasPrefix(value, "-")
	value = strings.TrimPrefix(strings.TrimPrefix(value, "-"), "+")
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" {
		return "", errors.New("invalid decimal syntax")
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if int64(len(fraction)) > scale {
		return "", errors.New("decimal scale mismatch")
	}
	digits := parts[0] + fraction + strings.Repeat("0", int(scale)-len(fraction))
	for _, digit := range digits {
		if digit < '0' || digit > '9' {
			return "", errors.New("decimal requires exact digits")
		}
	}
	digits = strings.TrimLeft(digits, "0")
	if digits == "" {
		digits = "0"
	}
	if int64(len(digits)) > precision {
		return "", errors.New("decimal precision mismatch")
	}
	if negative && digits != "0" {
		digits = "-" + digits
	}
	coefficient, ok := new(big.Int).SetString(digits, 10)
	if !ok {
		return "", errors.New("invalid decimal coefficient")
	}
	return coefficient.String(), nil
}

func encodeCell(column publicColumn, raw []byte) (cell, error) {
	if len(raw) > maxRowBytes {
		return cell{}, errors.New("public cell exceeds row bound")
	}
	typ := strings.ToUpper(column.SQLType)
	result := cell{Type: strings.ToLower(typ), Null: raw == nil}
	if raw == nil {
		return result, nil
	}
	value := string(raw)
	switch {
	case strings.Contains(typ, "DECIMAL") || typ == "NEWDECIMAL":
		if !column.DecimalKnown {
			return cell{}, errors.New("decimal metadata unavailable")
		}
		coefficient, err := exactDecimal(value, column.Precision, column.Scale)
		result.Type, result.Value, result.Scale = "decimal", coefficient, column.Scale
		return result, err
	case strings.Contains(typ, "INT") || typ == "YEAR":
		if strings.Contains(typ, "UNSIGNED") {
			parsed, err := strconv.ParseUint(value, 10, 64)
			result.Type, result.Value = "uint", strconv.FormatUint(parsed, 10)
			return result, err
		}
		parsed, err := strconv.ParseInt(value, 10, 64)
		result.Type, result.Value = "int", strconv.FormatInt(parsed, 10)
		return result, err
	case typ == "FLOAT" || typ == "DOUBLE":
		parsed, err := strconv.ParseFloat(value, 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return cell{}, errors.New("nonfinite or invalid approximate value")
		}
		result.Type, result.Value = "float", strconv.FormatFloat(parsed, 'g', -1, 64)
	case typ == "BIT" || strings.Contains(typ, "BLOB") || strings.Contains(typ, "BINARY"):
		result.Type, result.Value = "binary", base64.StdEncoding.EncodeToString(raw)
	default:
		if !utf8.Valid(raw) {
			return cell{}, errors.New("invalid UTF-8 public string")
		}
		result.Value = value
	}
	return result, nil
}

func run(ctx context.Context, cfg configuration, spec runSpec) (out output, err error) {
	if spec.Sequence < 1 || spec.Query < 1 || spec.Query > 22 || spec.SpoolBudget < 1 || spec.SpoolBudget > 4<<30 || (spec.Route != "mo-native" && spec.Route != "embedded-mo") {
		return out, errors.New("invalid run specification")
	}
	sqlBytes, err := os.ReadFile(filepath.Join(cfg.QueryDirectory, fmt.Sprintf("q%d.sql", spec.Query)))
	if err != nil {
		return out, err
	}
	digest := sha256.Sum256(sqlBytes)
	if hex.EncodeToString(digest[:]) != spec.QuerySHA256 {
		return out, errors.New("canonical SQL digest mismatch")
	}
	database := cfg.Databases[strconv.Itoa(spec.ScaleFactor)]
	if database == "" || strings.ContainsAny(database, "`\x00") {
		return out, errors.New("invalid dataset database")
	}
	driverCfg, err := mysql.ParseDSN(cfg.DSN)
	if err != nil {
		return out, err
	}
	if driverCfg.MultiStatements || driverCfg.AllowAllFiles || driverCfg.ParseTime {
		return out, errors.New("unsafe campaign driver options")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return out, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	connection, err := db.Conn(ctx)
	if err != nil {
		return out, err
	}
	defer connection.Close()
	for _, statement := range append([]string{"use `" + database + "`"}, cfg.Session...) {
		if _, err = connection.ExecContext(ctx, statement); err != nil {
			return out, err
		}
	}
	info, err := os.Lstat(cfg.Scratch)
	if err != nil {
		return out, err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return out, errors.New("scratch root is not private")
	}
	root, err := os.OpenRoot(cfg.Scratch)
	if err != nil {
		return out, err
	}
	defer root.Close()
	name := fmt.Sprintf("run-%08d-%s.ndjson", spec.Sequence, spec.Route)
	file, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return out, err
	}
	defer func() {
		_ = file.Close()
		if err != nil {
			_ = root.Remove(name)
		}
	}()
	statement := string(sqlBytes)
	if spec.Route == "embedded-mo" {
		statement = "/*+ SIDECAR GPU */ " + statement
	}
	started := time.Now()
	rows, err := connection.QueryContext(ctx, statement)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		return out, err
	}
	if len(columns) == 0 || len(columns) > 1024 {
		return out, errors.New("invalid public column count")
	}
	schema := make([]publicColumn, len(columns))
	raw := make([]sql.RawBytes, len(columns))
	arguments := make([]any, len(columns))
	for index, column := range columns {
		entry := publicColumn{Name: column.Name(), SQLType: column.DatabaseTypeName()}
		entry.Nullable, entry.NullableKnown = column.Nullable()
		entry.Precision, entry.Scale, entry.DecimalKnown = column.DecimalSize()
		schema[index] = entry
		arguments[index] = &raw[index]
	}
	hasher := sha256.New()
	writer := bufio.NewWriterSize(io.MultiWriter(file, hasher), 64<<10)
	var rowCount uint64
	var byteCount int64
	for rows.Next() {
		if out.FirstRow == nil {
			first := time.Since(started).Seconds()
			out.FirstRow = &first
		}
		if err = rows.Scan(arguments...); err != nil {
			return out, err
		}
		values := make([]cell, len(columns))
		for index := range raw {
			values[index], err = encodeCell(schema[index], raw[index])
			if err != nil {
				return out, err
			}
		}
		encoded, marshalErr := json.Marshal(values)
		if marshalErr != nil {
			return out, marshalErr
		}
		if len(encoded) > maxRowBytes || int64(len(encoded)+1) > spec.SpoolBudget-byteCount {
			return out, errors.New("row or spool quota exceeded")
		}
		if _, err = writer.Write(encoded); err != nil {
			return out, err
		}
		if err = writer.WriteByte('\n'); err != nil {
			return out, err
		}
		rowCount++
		byteCount += int64(len(encoded) + 1)
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if rows.NextResultSet() {
		return out, errors.New("unexpected additional result set")
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if err = rows.Close(); err != nil {
		return out, err
	}
	if err = writer.Flush(); err != nil {
		return out, err
	}
	if err = file.Close(); err != nil {
		return out, err
	}
	out.WallSeconds = time.Since(started).Seconds()
	if err = connection.QueryRowContext(ctx, "select last_query_id()").Scan(&out.QueryID); err != nil {
		return out, err
	}
	out.SchemaVersion, out.Sequence, out.Route = 2, spec.Sequence, spec.Route
	out.Result = resultDescriptor{Schema: schema, Rows: rowCount, Spool: name, Bytes: byteCount, SHA256: hex.EncodeToString(hasher.Sum(nil))}
	return out, nil
}

func main() {
	if len(os.Args) == 4 && os.Args[1] == "load" {
		cfg, err := readConfiguration(os.Getenv("MO_SIRIUS_CAMPAIGN_CONFIG"))
		if err != nil {
			_, _ = os.Stderr.WriteString("dataset configuration rejected\n")
			os.Exit(1)
		}
		ctx, cancel := context.WithTimeoutCause(context.Background(), 6*time.Hour, errors.New("dataset provisioning deadline"))
		defer cancel()
		result, err := loadDataset(ctx, cfg, os.Args[2], os.Args[3])
		if err != nil {
			_, _ = os.Stderr.WriteString("dataset provisioning failed\n")
			os.Exit(1)
		}
		if err = json.NewEncoder(os.Stdout).Encode(result); err != nil {
			os.Exit(1)
		}
		return
	}
	var spec runSpec
	input := os.Getenv("MO_SIRIUS_CAMPAIGN_RUN")
	cfg, err := readConfiguration(os.Getenv("MO_SIRIUS_CAMPAIGN_CONFIG"))
	if err == nil && len(input) <= 16<<10 {
		err = json.Unmarshal([]byte(input), &spec)
	} else if err == nil {
		err = errors.New("run specification too large")
	}
	if err != nil {
		_, _ = os.Stderr.WriteString("campaign configuration rejected\n")
		os.Exit(1)
	}
	parent, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeoutCause(parent, time.Duration(cfg.TimeoutSeconds)*time.Second, errors.New("campaign query deadline"))
	defer cancel()
	out, err := run(ctx, cfg, spec)
	if err != nil {
		// Neither DSNs, SQL, paths nor driver-controlled diagnostics cross stdout.
		_, _ = os.Stderr.WriteString("campaign query failed\n")
		os.Exit(1)
	}
	if err = json.NewEncoder(os.Stdout).Encode(out); err != nil {
		os.Exit(1)
	}
}
