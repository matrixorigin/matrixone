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
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	"github.com/go-sql-driver/mysql"
)

type datasetTable struct {
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
	Bytes  int64  `json:"bytes"`
	Rows   uint64 `json:"row_count"`
}

type datasetManifest struct {
	Version int                     `json:"schema_version"`
	Scale   int                     `json:"scale_factor"`
	DDL     string                  `json:"ddl_sha256"`
	Digest  string                  `json:"dataset_sha256"`
	Tables  map[string]datasetTable `json:"tables"`
}

// Commutative cryptographic row identity avoids imposing a benchmark ORDER BY
// on a provisioning scan. Count plus 256-bit sum/XOR preserve multiplicities.
type rowIdentity struct {
	count uint64
	sum   big.Int
	xor   [32]byte
}

func (identity *rowIdentity) add(row []cell) error {
	data, err := json.Marshal(row)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(data)
	var value big.Int
	value.SetBytes(digest[:])
	identity.sum.Add(&identity.sum, &value)
	identity.sum.Mod(&identity.sum, new(big.Int).Lsh(big.NewInt(1), 256))
	for index := range digest {
		identity.xor[index] ^= digest[index]
	}
	identity.count++
	return nil
}

func (identity *rowIdentity) digest() string {
	var value [72]byte
	binary.BigEndian.PutUint64(value[:8], identity.count)
	identity.sum.FillBytes(value[8:40])
	copy(value[40:], identity.xor[:])
	digest := sha256.Sum256(value[:])
	return hex.EncodeToString(digest[:])
}

func fileDigest(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func loadDataset(ctx context.Context, cfg configuration, manifestPath, database string) (map[string]any, error) {
	if strings.ContainsAny(database, "`\x00") || database == "" {
		return nil, errors.New("invalid database")
	}
	if err := privateFile(manifestPath); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(manifestPath)
	if err != nil {
		return nil, err
	}
	var manifest datasetManifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	if manifest.Version != 2 || len(manifest.Tables) != 8 {
		return nil, errors.New("invalid dataset manifest")
	}
	root := filepath.Dir(manifestPath)
	ddlPath := filepath.Join(root, "schema.sql")
	actualDDL, err := fileDigest(ddlPath)
	if err != nil || actualDDL != manifest.DDL {
		return nil, errors.New("dataset DDL identity mismatch")
	}
	db, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	connection, err := db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer connection.Close()
	if _, err = connection.ExecContext(ctx, "create database `"+database+"`"); err != nil {
		return nil, err
	}
	if _, err = connection.ExecContext(ctx, "use `"+database+"`"); err != nil {
		return nil, err
	}
	ddl, err := os.ReadFile(ddlPath)
	if err != nil {
		return nil, err
	}
	for _, statement := range strings.Split(string(ddl), ";") {
		if strings.TrimSpace(statement) != "" {
			if _, err = connection.ExecContext(ctx, statement); err != nil {
				return nil, err
			}
		}
	}
	results := make(map[string]any, 8)
	for _, table := range []string{"nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"} {
		entry, exists := manifest.Tables[table]
		if !exists || filepath.Base(entry.File) != entry.File {
			return nil, errors.New("invalid dataset table")
		}
		path := filepath.Join(root, entry.File)
		digest, err := fileDigest(path)
		if err != nil || digest != entry.SHA256 {
			return nil, errors.New("source dataset checksum mismatch")
		}
		mysql.RegisterLocalFile(path)
		statement := "load data local infile '" + strings.ReplaceAll(path, "'", "''") + "' into table " + table + " fields terminated by '|' enclosed by '\"' lines terminated by '\\n'"
		_, err = connection.ExecContext(ctx, statement)
		mysql.DeregisterLocalFile(path)
		if err != nil {
			return nil, err
		}
		identity, err := verifyDatasetTable(ctx, connection, table, path, entry.Rows)
		if err != nil {
			return nil, err
		}
		results[table] = map[string]any{"row_count": entry.Rows, "source_sha256": entry.SHA256, "loaded_data_sha256": identity}
	}
	return map[string]any{"schema_version": 2, "dataset_sha256": manifest.Digest, "scale_factor": manifest.Scale, "tables": results}, nil
}

func verifyDatasetTable(ctx context.Context, connection *sql.Conn, table, sourcePath string, expectedRows uint64) (string, error) {
	rows, err := connection.QueryContext(ctx, "select * from "+table)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	columns, err := rows.ColumnTypes()
	if err != nil {
		return "", err
	}
	schema := make([]publicColumn, len(columns))
	raw := make([]sql.RawBytes, len(columns))
	arguments := make([]any, len(columns))
	for index, column := range columns {
		schema[index] = publicColumn{Name: column.Name(), SQLType: column.DatabaseTypeName()}
		schema[index].Precision, schema[index].Scale, schema[index].DecimalKnown = column.DecimalSize()
		arguments[index] = &raw[index]
	}
	var actual rowIdentity
	for rows.Next() {
		if err = rows.Scan(arguments...); err != nil {
			return "", err
		}
		values := make([]cell, len(columns))
		for index := range raw {
			values[index], err = encodeCell(schema[index], raw[index])
			if err != nil {
				return "", err
			}
		}
		if err = actual.add(values); err != nil {
			return "", err
		}
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if err = rows.Close(); err != nil {
		return "", err
	}
	file, err := os.Open(sourcePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	reader.Comma = '|'
	reader.FieldsPerRecord = len(columns)
	reader.ReuseRecord = true
	var expected rowIdentity
	for {
		fields, readErr := reader.Read()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", readErr
		}
		values := make([]cell, len(fields))
		for index, value := range fields {
			var bytes []byte
			if value != "\\N" {
				bytes = []byte(value)
			}
			values[index], err = encodeCell(schema[index], bytes)
			if err != nil {
				return "", err
			}
		}
		if err = expected.add(values); err != nil {
			return "", err
		}
	}
	if actual.count != expectedRows || expected.count != expectedRows || actual.digest() != expected.digest() {
		return "", errors.New("loaded dataset semantic identity mismatch")
	}
	return actual.digest(), nil
}
