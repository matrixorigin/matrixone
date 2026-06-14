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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestValidateWriteFilePattern(t *testing.T) {
	ctx := context.Background()

	// read-only table: no option => ok
	require.NoError(t, validateWriteFilePattern(ctx, &tree.ExternParam{}, nil))

	// valid csv write pattern
	p := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "stage://s/part-%U.csv"},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p, nil))

	// valid jsonline, format taken from Option
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "write_file_pattern", "stage://s/part-%6N.jl"},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p, nil))

	// not a stage path
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "/tmp/part-%U.csv"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))

	// unsupported format
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.PARQUET,
		Option: []string{"write_file_pattern", "stage://s/part-%U.pq"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))

	// bad strftime directive
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "stage://s/part-%Q.csv"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))

	// jsonline with jsondata 'array' is not writable (writer emits objects)
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "array", "write_file_pattern", "stage://s/part-%U.jl"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))

	// jsonline with jsondata from the materialized field
	p = &tree.ExternParam{
		ExParamConst: tree.ExParamConst{
			Format: tree.JSONLINE,
			Option: []string{"write_file_pattern", "stage://s/part-%U.jl"},
		},
		ExParam: tree.ExParam{JsonData: tree.ARRAY},
	}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))

	// jsonline cannot use a printable line terminator: JSON strings have no
	// enclosure, so data containing it would split records
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "write_file_pattern", "stage://s/part-%U.jl"},
		Tail: &tree.TailParameter{
			Lines: &tree.Lines{TerminatedBy: &tree.Terminated{Value: "#"}},
		},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))
	// ... while \r\n stays allowed (control chars are escaped in JSON strings)
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "write_file_pattern", "stage://s/part-%U.jl"},
		Tail: &tree.TailParameter{
			Lines: &tree.Lines{TerminatedBy: &tree.Terminated{Value: "\r\n"}},
		},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p, nil))

	// jsonline does not accept a COMMENT marker: every record begins with '{'
	// (the reader matches the marker on the raw line prefix), so a marker like
	// '{' would skip every written row and JSON has no enclosure to hide it
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "comment", "{", "write_file_pattern", "stage://s/part-%U.jl"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))
	// any non-empty comment is rejected for jsonline, not just '{'
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "comment", "#", "write_file_pattern", "stage://s/part-%U.jl"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))
	// an empty comment (the default — no marker) stays allowed for jsonline
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "comment", "", "write_file_pattern", "stage://s/part-%U.jl"},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p, nil))

	// jsonline with jsondata 'object' stays writable
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "write_file_pattern", "stage://s/part-%U.jl"},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p, nil))

	// pattern without a %U/%nN uniqueness directive: parallel writers would
	// expand to the same path, rejected at DDL time
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "stage://s/out-%Y%m%d.csv"},
	}}
	err := validateWriteFilePattern(ctx, p, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "%U")

	// %%U is a literal "%U", not a uniqueness directive
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "stage://s/out-%%U.csv"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))
}

func TestValidateWriteFilePatternCompression(t *testing.T) {
	ctx := context.Background()

	// explicit gzip compression: the writer emits plain bytes, rejected
	p := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"compression", "gzip", "filepath", "stage://s/f_*.csv", "write_file_pattern", "stage://s/f_%U.csv"},
	}}
	err := validateWriteFilePattern(ctx, p, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "compression")

	// compression auto-detected from the read FILEPATH suffix
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"filepath", "stage://s/f_*.csv.gz", "write_file_pattern", "stage://s/f_%U.csv"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))

	// ... and from the write pattern's own suffix
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"filepath", "stage://s/f_*.csv", "write_file_pattern", "stage://s/f_%U.csv.gz"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))

	// explicit 'none' is fine
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"compression", "none", "filepath", "stage://s/f_*.csv", "write_file_pattern", "stage://s/f_%U.csv"},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, p, nil))
}

func TestValidateWriteFilePatternDuplicateKeys(t *testing.T) {
	ctx := context.Background()
	// duplicate format: validation would see the first, the read init keeps the
	// last — rejected so the two can't disagree
	p := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"format", "csv", "format", "parquet", "write_file_pattern", "stage://s/f_%U.csv"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))

	// duplicate jsondata
	p = &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "jsondata", "array", "write_file_pattern", "stage://s/f_%U.jl"},
	}}
	require.Error(t, validateWriteFilePattern(ctx, p, nil))
}

func TestValidateWriteFilePatternFieldTerminator(t *testing.T) {
	ctx := context.Background()
	mk := func(term string) *tree.ExternParam {
		return &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Format: tree.CSV,
			Option: []string{"write_file_pattern", "stage://s/f_%U.csv"},
			Tail:   &tree.TailParameter{Fields: &tree.Fields{Terminated: &tree.Terminated{Value: term}}},
		}}
	}
	// the reader rejects a field terminator starting with quote/CR/LF
	require.Error(t, validateWriteFilePattern(ctx, mk("\""), nil))
	require.Error(t, validateWriteFilePattern(ctx, mk("\r"), nil))
	require.Error(t, validateWriteFilePattern(ctx, mk("\n"), nil))
	// '#' is a valid separator (no comment marker is configured by default)
	require.NoError(t, validateWriteFilePattern(ctx, mk("#"), nil))
	require.NoError(t, validateWriteFilePattern(ctx, mk("|"), nil))
}

func TestValidateWriteFilePatternColumns(t *testing.T) {
	ctx := context.Background()
	csvParam := func() *tree.ExternParam {
		return &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Format: tree.CSV,
			Option: []string{"write_file_pattern", "stage://s/part-%U.csv"},
		}}
	}

	// plain columns are fine; hidden synthetic columns (like the fake-PK column
	// added to tables without a primary key) are skipped even when AutoIncr.
	td := &TableDef{Cols: []*ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: "__mo_fake_pk_col", Hidden: true, Typ: plan.Type{Id: int32(types.T_uint64), AutoIncr: true}},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, csvParam(), td))

	// AUTO_INCREMENT is generated by PreInsert, which the external plan skips
	td = &TableDef{Cols: []*ColDef{
		{Name: "id", Typ: plan.Type{Id: int32(types.T_int64), AutoIncr: true}},
	}}
	err := validateWriteFilePattern(ctx, csvParam(), td)
	require.Error(t, err)
	require.Contains(t, err.Error(), "AUTO_INCREMENT")

	// bit columns cannot round-trip through JSON strings
	jlParam := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Option: []string{"format", "jsonline", "jsondata", "object", "write_file_pattern", "stage://s/part-%U.jl"},
	}}
	td = &TableDef{Cols: []*ColDef{
		{Name: "b", Typ: plan.Type{Id: int32(types.T_bit), Width: 8}},
	}}
	err = validateWriteFilePattern(ctx, jlParam, td)
	require.Error(t, err)
	require.Contains(t, err.Error(), "bit")

	// bit is fine for csv (enclosed + escaped like binary)
	require.NoError(t, validateWriteFilePattern(ctx, csvParam(), td))

	// binary/varbinary/blob cannot round-trip through jsonline either: the
	// writer would base64 them but the reader does not decode
	for _, typ := range []types.T{types.T_binary, types.T_varbinary, types.T_blob} {
		jl := &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Option: []string{"format", "jsonline", "jsondata", "object", "write_file_pattern", "stage://s/part-%U.jl"},
		}}
		btd := &TableDef{Cols: []*ColDef{{Name: "x", Typ: plan.Type{Id: int32(typ)}}}}
		err = validateWriteFilePattern(ctx, jl, btd)
		require.Error(t, err, typ.String())
		// same columns are fine for csv (raw bytes enclosed + escaped)
		require.NoError(t, validateWriteFilePattern(ctx, csvParam(), btd), typ.String())
	}
}

func TestValidateWriteFilePatternComment(t *testing.T) {
	ctx := context.Background()
	mk := func(opt ...string) *tree.ExternParam {
		return &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Format: tree.CSV,
			Option: append([]string{"write_file_pattern", "stage://s/part-%U.csv"}, opt...),
		}}
	}

	mkTail := func(comment string, tail *tree.TailParameter) *tree.ExternParam {
		p := mk("comment", comment)
		p.Tail = tail
		return p
	}

	// A COMMENT marker is accepted on writable tables: the writer encloses the
	// first field of any colliding row so it reads back as data (see the
	// externalwrite encoder's firstFieldStartsComment guard).
	require.NoError(t, validateWriteFilePattern(ctx, mk(), nil))
	require.NoError(t, validateWriteFilePattern(ctx, mk("comment", ""), nil))
	require.NoError(t, validateWriteFilePattern(ctx, mk("comment", "#"), nil))
	require.NoError(t, validateWriteFilePattern(ctx, mk("comment", "REM"), nil))

	// COMMENT and LINES STARTING BY are mutually exclusive: the reader matches the
	// marker on the raw prefix before consuming STARTING BY, so 'REM' + 'REM:'
	// would skip every row.
	require.Error(t, validateWriteFilePattern(ctx, mkTail("REM", &tree.TailParameter{
		Lines: &tree.Lines{StartingBy: "REM:"},
	}), nil))

	// COMMENT must not begin with the enclosure byte: an enclosed first field
	// starts the line with it, so '"' would skip those rows (enclosing is the
	// collision, not the fix).
	require.Error(t, validateWriteFilePattern(ctx, mk("comment", `"`), nil))
	// ... including a custom ENCLOSED BY.
	require.Error(t, validateWriteFilePattern(ctx, mkTail("|", &tree.TailParameter{
		Fields: &tree.Fields{EnclosedBy: &tree.EnclosedBy{Value: '|'}},
	}), nil))

	// COMMENT must not begin with the escape byte (default '\\'): the writer can
	// emit an unenclosed first field starting with a doubled escape.
	require.Error(t, validateWriteFilePattern(ctx, mk("comment", `\`), nil))
	// ... including a custom FIELDS ESCAPED BY.
	require.Error(t, validateWriteFilePattern(ctx, mkTail("!x", &tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: '!'}},
	}), nil))
	// a non-structural marker byte stays valid alongside a custom escape.
	require.NoError(t, validateWriteFilePattern(ctx, mkTail("@", &tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: '!'}},
	}), nil))

	// COMMENT must not begin with the field terminator: an empty first field
	// makes the line start with it.
	require.Error(t, validateWriteFilePattern(ctx, mk("comment", ","), nil))
	// ... including a custom FIELDS TERMINATED BY.
	require.Error(t, validateWriteFilePattern(ctx, mkTail("|x", &tree.TailParameter{
		Fields: &tree.Fields{Terminated: &tree.Terminated{Value: "|"}},
	}), nil))
	// '#' stays a valid marker with the default ',' terminator (the main use case).
	require.NoError(t, validateWriteFilePattern(ctx, mkTail("#", &tree.TailParameter{
		Fields: &tree.Fields{Terminated: &tree.Terminated{Value: ","}},
	}), nil))

	// COMMENT must not collide with the NULL sentinel \N (written verbatim for a
	// NULL first column, with a literal backslash regardless of the escape).
	require.Error(t, validateWriteFilePattern(ctx, mk("comment", `\N`), nil))
	require.Error(t, validateWriteFilePattern(ctx, mkTail(`\N`, &tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: '!'}},
	}), nil))
}

func TestValidateWriteFilePatternGeneratedColumn(t *testing.T) {
	ctx := context.Background()
	csvParam := &tree.ExternParam{ExParamConst: tree.ExParamConst{
		Format: tree.CSV,
		Option: []string{"write_file_pattern", "stage://s/part-%U.csv"},
	}}

	// a generated column is rejected: the minimal external INSERT/LOAD plan does
	// not run the generated-column rewrite, so it would store arbitrary or
	// NULL/default values instead of the computed expression.
	td := &TableDef{Cols: []*ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
		{Name: "g", Typ: plan.Type{Id: int32(types.T_int32)}, GeneratedCol: &plan.GeneratedCol{}},
	}}
	err := validateWriteFilePattern(ctx, csvParam, td)
	require.Error(t, err)
	require.Contains(t, err.Error(), "generated column")

	// plain columns alone are fine
	td = &TableDef{Cols: []*ColDef{
		{Name: "a", Typ: plan.Type{Id: int32(types.T_int32)}},
	}}
	require.NoError(t, validateWriteFilePattern(ctx, csvParam, td))
}

func TestValidateWriteFilePatternTail(t *testing.T) {
	ctx := context.Background()
	withTail := func(tail *tree.TailParameter) *tree.ExternParam {
		return &tree.ExternParam{ExParamConst: tree.ExParamConst{
			Format: tree.CSV,
			Option: []string{"write_file_pattern", "stage://s/part-%U.csv"},
			Tail:   tail,
		}}
	}

	// default escape ('\') explicit or absent is fine
	require.NoError(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: '\\'}},
	}), nil))
	require.NoError(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{}), nil))

	// custom escape chars are supported by the writer (doubled on write)
	require.NoError(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: '!'}},
	}), nil))
	// ... as is explicitly disabled escaping (ESCAPED BY '')
	require.NoError(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: 0}},
	}), nil))

	// but not characters the reader's unescaper maps to control characters
	// (E+E would decode to \n, not E) ...
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: 'n'}},
	}), nil))
	// ... or the enclosure character (explicit or the '"' default) ...
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: '"'}},
	}), nil))
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{
			EscapedBy:  &tree.EscapedBy{Value: '|'},
			EnclosedBy: &tree.EnclosedBy{Value: '|'},
		},
	}), nil))
	// ... or a byte of the field/line terminator
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{
			EscapedBy:  &tree.EscapedBy{Value: ';'},
			Terminated: &tree.Terminated{Value: ";"},
		},
	}), nil))

	// the enclosure conflict applies to the DEFAULT backslash escape too
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{EnclosedBy: &tree.EnclosedBy{Value: '\\'}},
	}), nil))
	// control characters cannot be the escape (they collide with the writer's
	// own CR encoding and the reader's record handling)
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{EscapedBy: &tree.EscapedBy{Value: 0x01}},
	}), nil))

	// a custom enclosure is fine ...
	require.NoError(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{EnclosedBy: &tree.EnclosedBy{Value: '|'}},
	}), nil))
	// ... unless it occurs in a terminator (tokenizer ambiguity no quoting
	// fixes): explicit enclosure inside the explicit field terminator,
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{
			EnclosedBy: &tree.EnclosedBy{Value: 'x'},
			Terminated: &tree.Terminated{Value: "x-"},
		},
	}), nil))
	// or the default '"' enclosure inside a custom terminator.
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Fields: &tree.Fields{Terminated: &tree.Terminated{Value: `"`}},
	}), nil))

	// IGNORE N LINES would discard real data rows on readback
	require.Error(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		IgnoredLines: 1,
	}), nil))

	// LINES STARTING BY is supported: the writer emits the prefix
	require.NoError(t, validateWriteFilePattern(ctx, withTail(&tree.TailParameter{
		Lines: &tree.Lines{StartingBy: "row:"},
	}), nil))
}

// TestEffectiveWriteCompression covers the compression detection helper: an
// explicit non-auto type wins; otherwise the type is auto-detected from any
// path suffix; empty/auto/none with a plain suffix means uncompressed.
func TestEffectiveWriteCompression(t *testing.T) {
	// explicit compression wins regardless of suffix
	eff, yes := effectiveWriteCompression("gzip", "x.csv", "y.csv")
	require.True(t, yes)
	require.Equal(t, "gzip", eff)

	// explicit none is uncompressed even with a .gz suffix
	_, yes = effectiveWriteCompression("none", "x.csv.gz")
	require.False(t, yes)

	// auto-detect from suffix on any of the paths
	for _, suf := range []string{".gz", ".gzip", ".bz2", ".bzip2", ".lz4", ".tar.gz", ".tar.bz2"} {
		_, yes := effectiveWriteCompression("", "plain.csv", "out"+suf)
		require.True(t, yes, suf)
	}

	// auto keyword falls through to suffix detection
	_, yes = effectiveWriteCompression("auto", "x.csv.bz2")
	require.True(t, yes)

	// no compression: empty option, plain suffixes
	eff, yes = effectiveWriteCompression("", "a.csv", "b.csv")
	require.False(t, yes)
	require.Equal(t, "none", eff)
}
