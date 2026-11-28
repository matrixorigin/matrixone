// Copyright 2026 Matrix Origin
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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

// PR 32906 reviews 5983418028/5983638763: every newly admitted CTAS table
// option must be reserialized in the grammar-accepted shape with the same
// decoded value when mo_catalog.mo_tables.rel_createsql replays it.

func roundtripAdmittedOptionCTAS(t *testing.T, optionSuffix, serializedMust string) *tree.CreateTable {
	t.Helper()
	sql := "CREATE TABLE tq " + optionSuffix
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	create, ok := stmt.(*tree.CreateTable)
	require.True(t, ok, sql)
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	createSQL := createTableSQLForCatalog(ctx, create)
	require.NotEmpty(t, createSQL, sql)
	require.Contains(t, createSQL, serializedMust, createSQL)
	reStmt, reErr := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
	require.NoError(t, reErr, createSQL)
	reCreate, ok := reStmt.(*tree.CreateTable)
	require.True(t, ok, createSQL)
	return reCreate
}

func requireCompression(t *testing.T, create *tree.CreateTable, sql, want string) {
	t.Helper()
	for _, o := range create.Options {
		if v, ok := o.(*tree.TableOptionCompression); ok {
			require.Equal(t, want, v.Compression, sql)
		}
	}
}

func TestCTASAdmittedOptionLiteralsRelCreateSQLRoundtrip(t *testing.T) {
	t.Run("compression", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "COMPRESSION 'zlib' AS SELECT 1 AS a", "compression = 'zlib'")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionCompression); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		requireCompression(t, reCreate, "reparse", "zlib")
	})
	t.Run("compression_escaped", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "COMPRESSION 'z li''b\\\\' AS SELECT 1 AS a", "compression = 'z li''b\\\\'")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionCompression); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		requireCompression(t, reCreate, "reparse", "z li'b\\")
	})
	t.Run("encryption", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "ENCRYPTION 'Y' AS SELECT 1 AS a", "encryption = 'Y'")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionEncryption); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionEncryption); ok {
				require.Equal(t, "Y", v.Encryption, "reparse")
			}
		}
	})
	t.Run("delay_key_write", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "DELAY_KEY_WRITE 1 AS SELECT 1 AS a", "delay_key_write = 1")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionDelayKeyWrite); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		present := false
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionDelayKeyWrite); ok {
				present = true
				require.Equal(t, uint64(1), v.Value, "reparse")
			}
		}
		require.True(t, present, "delay_key_write option must survive the roundtrip")
	})
	t.Run("storage_disk", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "STORAGE DISK AS SELECT 1 AS a", "STORAGE DISK")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionTablespace); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionTablespace); ok {
				require.Equal(t, "", v.Name, "reparse")
				require.Equal(t, " STORAGE DISK", v.StorageOpt, "reparse")
			}
		}
	})
	t.Run("storage_memory", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "STORAGE MEMORY AS SELECT 1 AS a", "STORAGE MEMORY")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionTablespace); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionTablespace); ok {
				require.Equal(t, " STORAGE MEMORY", v.StorageOpt, "reparse")
			}
		}
	})
	t.Run("tablespace_ident", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "TABLESPACE ts1 AS SELECT 1 AS a", "tablespace = `ts1`")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionTablespace); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionTablespace); ok {
				require.Equal(t, "ts1", v.Name, "reparse")
				require.Equal(t, "", v.StorageOpt, "reparse")
			}
		}
	})
	t.Run("connection", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "CONNECTION 'c one' AS SELECT 1 AS a", "connection = 'c one'")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionConnection); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionConnection); ok {
				require.Equal(t, "c one", v.Connection, "reparse")
			}
		}
	})
	t.Run("data_directory", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "DATA DIRECTORY '/data/x' AS SELECT 1 AS a", "data directory = '/data/x'")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionDataDirectory); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionDataDirectory); ok {
				require.Equal(t, "/data/x", v.Dir, "reparse")
			}
		}
	})
	t.Run("index_directory", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "INDEX DIRECTORY '/idx/x' AS SELECT 1 AS a", "index directory = '/idx/x'")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionIndexDirectory); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionIndexDirectory); ok {
				require.Equal(t, "/idx/x", v.Dir, "reparse")
			}
		}
	})
	t.Run("password", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "PASSWORD 'se''cret' AS SELECT 1 AS a", "password = 'se''cret'")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionPassword); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionPassword); ok {
				require.Equal(t, "se'cret", v.Password, "reparse")
			}
		}
	})
	t.Run("tablespace_quoted_identifier", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "TABLESPACE `ts archive` AS SELECT 1 AS a", "tablespace = `ts archive`")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionTablespace); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "tablespace typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionTablespace); ok {
				require.Equal(t, "ts archive", v.Name, "reparse")
			}
		}
	})
	t.Run("engine_spaced_value_identifier_form", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "ENGINE 'test engine' AS SELECT 1 AS a", "engine = `test engine`")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionEngine); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "engine typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionEngine); ok {
				require.Equal(t, "test engine", v.Engine, "reparse")
			}
		}
	})
	t.Run("tablespace_reserved_word", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "TABLESPACE `select` AS SELECT 1 AS a", "tablespace = `select`")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionTablespace); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "tablespace typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionTablespace); ok {
				require.Equal(t, "select", v.Name, "reparse")
			}
		}
	})
	t.Run("engine_reserved_word", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "ENGINE 'select' AS SELECT 1 AS a", "engine = `select`")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionEngine); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "engine typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionEngine); ok {
				require.Equal(t, "select", v.Engine, "reparse")
			}
		}
	})
	t.Run("engine_empty_string_value", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "ENGINE '' AS SELECT 1 AS a", "engine = ''")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionEngine); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "engine typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionEngine); ok {
				require.Equal(t, "", v.Engine, "reparse")
			}
		}
	})
	t.Run("engine_plain_identifier_roundtrip", func(t *testing.T) {
		reCreate := roundtripAdmittedOptionCTAS(t, "ENGINE innodb AS SELECT 1 AS a", "engine = `innodb`")
		found := 0
		for _, o := range reCreate.Options {
			if _, ok := o.(*tree.TableOptionEngine); ok {
				found++
			}
		}
		require.Equal(t, 1, found, "engine typed option must be present exactly once")
		for _, o := range reCreate.Options {
			if v, ok := o.(*tree.TableOptionEngine); ok {
				require.Equal(t, "innodb", v.Engine, "reparse")
			}
		}
	})
}

func TestCTASAllAdmittedOptionsCombinedRoundtrip(t *testing.T) {
	sql := "CREATE TABLE tq COMPRESSION 'zlib' ENCRYPTION 'Y' DELAY_KEY_WRITE 1 STORAGE DISK " +
		"CONNECTION 'c one' DATA DIRECTORY '/data/x' INDEX DIRECTORY '/idx/x' PASSWORD 'se''cret' " +
		"TABLESPACE ts1 AS SELECT 1 AS a"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	create, ok := stmt.(*tree.CreateTable)
	require.True(t, ok, sql)
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	createSQL := createTableSQLForCatalog(ctx, create)
	for _, must := range []string{
		"compression = 'zlib'", "encryption = 'Y'", "delay_key_write = 1",
		"STORAGE DISK", "connection = 'c one'", "data directory = '/data/x'",
		"index directory = '/idx/x'", "password = 'se''cret'", "tablespace = `ts1`",
	} {
		require.Contains(t, createSQL, must, createSQL)
	}
	reStmt, reErr := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
	require.NoError(t, reErr, createSQL)
	reCreate, ok := reStmt.(*tree.CreateTable)
	require.True(t, ok, createSQL)
	counts := map[string]int{}
	for _, o := range reCreate.Options {
		switch v := o.(type) {
		case *tree.TableOptionCompression:
			counts["compression"]++
			require.Equal(t, "zlib", v.Compression, createSQL)
		case *tree.TableOptionEncryption:
			counts["encryption"]++
			require.Equal(t, "Y", v.Encryption, createSQL)
		case *tree.TableOptionDelayKeyWrite:
			counts["delay_key_write"]++
			require.Equal(t, uint64(1), v.Value, createSQL)
		case *tree.TableOptionTablespace:
			if v.Name != "" {
				counts["tablespace_ident"]++
				require.Equal(t, "ts1", v.Name, createSQL)
				require.Equal(t, "", v.StorageOpt, createSQL)
			} else {
				counts["storage_opt"]++
				require.Equal(t, " STORAGE DISK", v.StorageOpt, createSQL)
			}
		case *tree.TableOptionConnection:
			counts["connection"]++
			require.Equal(t, "c one", v.Connection, createSQL)
		case *tree.TableOptionDataDirectory:
			counts["data_directory"]++
			require.Equal(t, "/data/x", v.Dir, createSQL)
		case *tree.TableOptionIndexDirectory:
			counts["index_directory"]++
			require.Equal(t, "/idx/x", v.Dir, createSQL)
		case *tree.TableOptionPassword:
			counts["password"]++
			require.Equal(t, "se'cret", v.Password, createSQL)
		}
	}
	require.Exactly(t, map[string]int{
		"compression": 1, "encryption": 1, "delay_key_write": 1, "connection": 1,
		"data_directory": 1, "index_directory": 1, "password": 1,
		"storage_opt": 1, "tablespace_ident": 1,
	}, counts, createSQL)
}

func TestPlainCreateAdmittedOptionLiteralRoundtrip(t *testing.T) {
	sql := "CREATE TABLE pl (a INT) COMPRESSION 'zlib'"
	stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, sql, 1)
	require.NoError(t, err, sql)
	create, ok := stmt.(*tree.CreateTable)
	require.True(t, ok, sql)
	ctx := NewMockCompilerContext(false, newPlanTestProcess(t))
	createSQL := createTableSQLForCatalog(ctx, create)
	require.Contains(t, createSQL, "compression = 'zlib'", createSQL)
	reStmt, reErr := parsers.ParseOne(t.Context(), dialect.MYSQL, createSQL, 1)
	require.NoError(t, reErr, createSQL)
	reCreate, ok := reStmt.(*tree.CreateTable)
	require.True(t, ok, createSQL)
	requireCompression(t, reCreate, createSQL, "zlib")
}
