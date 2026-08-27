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

package frontend

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
)

// TestNewSQLErrorCarriesCodes covers what the value exists for: a procedure
// branching on an error CLASS instead of matching on message text.
func TestNewSQLErrorCarriesCodes(t *testing.T) {
	dup := moerr.NewDuplicateEntryNoCtx("1", "PRIMARY")
	e := newSQLError(dup)
	require.Equal(t, dup.MySQLCode(), e.code)
	require.Equal(t, uint16(1062), e.code)
	require.Equal(t, "23000", e.sqlstate)
	require.Equal(t, dup.Error(), e.message)

	// wrapped moerrs are still recognized: errors.As, not a type assertion
	e = newSQLError(moerr.AttachCause(context.Background(), dup))
	require.Equal(t, uint16(1062), e.code)

	// a failure that is not a moerr at all reports code 0 rather than
	// pretending to be some SQL error
	e = newSQLError(errors.New("argument unpacking failed"))
	require.Equal(t, uint16(0), e.code)
	require.Equal(t, "", e.sqlstate)
	require.Equal(t, "argument unpacking failed", e.message)
}

// TestSQLErrorStaysAString pins the compatibility half: every use that worked
// when this was a plain string still works.
func TestSQLErrorStaysAString(t *testing.T) {
	e := newSQLError(moerr.NewDuplicateEntryNoCtx("1", "PRIMARY"))

	require.Equal(t, e.message, e.String())
	require.Equal(t, "mo.error", e.Type())
	require.NotPanics(t, e.Freeze)
	require.Equal(t, starlark.True, e.Truth())

	h, err := e.Hash()
	require.NoError(t, err)
	sh, err := starlark.String(e.message).Hash()
	require.NoError(t, err)
	require.Equal(t, sh, h, "hashes like the string it replaced")

	// an empty message is falsy, so `if err:` keeps meaning "did it fail"
	require.Equal(t, starlark.False, (&sqlError{}).Truth())

	// concatenation, both ways round
	v, err := e.Binary(syntax.PLUS, starlark.String(" !"), starlark.Left)
	require.NoError(t, err)
	require.Equal(t, starlark.String(e.message+" !"), v)

	v, err = e.Binary(syntax.PLUS, starlark.String("oops: "), starlark.Right)
	require.NoError(t, err)
	require.Equal(t, starlark.String("oops: "+e.message), v)

	// anything else defers to starlark's own handling (nil, nil), rather than
	// inventing a result
	v, err = e.Binary(syntax.MINUS, starlark.String("x"), starlark.Left)
	require.NoError(t, err)
	require.Nil(t, v)
	v, err = e.Binary(syntax.PLUS, starlark.MakeInt(1), starlark.Left)
	require.NoError(t, err)
	require.Nil(t, v)
}

func TestSQLErrorAttrs(t *testing.T) {
	e := newSQLError(moerr.NewDuplicateEntryNoCtx("1", "PRIMARY"))

	code, err := e.Attr("code")
	require.NoError(t, err)
	require.Equal(t, starlark.MakeInt(1062), code)

	state, err := e.Attr("sqlstate")
	require.NoError(t, err)
	require.Equal(t, starlark.String("23000"), state)

	msg, err := e.Attr("message")
	require.NoError(t, err)
	require.Equal(t, starlark.String(e.message), msg)

	// an attribute that is not one of ours is asked of the message, because
	// this value used to BE that string: a procedure written against the old
	// return value calls err.startswith(...) and must keep working
	starts, err := e.Attr("startswith")
	require.NoError(t, err)
	require.NotNil(t, starts, "string methods must survive")

	res, err := starlark.Call(&starlark.Thread{}, starts,
		starlark.Tuple{starlark.String("Duplicate")}, nil)
	require.NoError(t, err)
	require.Equal(t, starlark.Bool(strings.HasPrefix(e.message, "Duplicate")), res)

	split, err := e.Attr("split")
	require.NoError(t, err)
	require.NotNil(t, split)

	// and dir(err) lists both halves, so the codes are discoverable without
	// hiding what the string could already do
	names := e.AttrNames()
	require.Subset(t, names, []string{"code", "message", "sqlstate"})
	require.Subset(t, names, starlark.String("").AttrNames())

	// something that is neither still yields starlark's own "no .x field"
	v, err := e.Attr("definitely_not_a_method")
	require.NoError(t, err)
	require.Nil(t, v)
}

// TestSQLErrorConvertsToItsMessage covers the OUT-parameter path: assigning
// the error to a procedure output yields the text, as it did before the value
// grew its codes.
func TestSQLErrorConvertsToItsMessage(t *testing.T) {
	e := newSQLError(moerr.NewDuplicateEntryNoCtx("1", "PRIMARY"))
	got, err := convertFromStarlarkValue(context.Background(), e)
	require.NoError(t, err)
	require.Equal(t, e.message, got)
}

// TestMoBuiltinsReportBadArgsAsSQLError covers the [result, ok] contract on
// the path every mo.* builtin shares: a bad call reports the failure in the
// `ok` slot as an mo.error and returns no Go error, so a procedure sees a
// value it can branch on rather than an aborted script.
//
// Argument unpacking fails before any builtin touches the session, which is
// why a bare interpreter can drive all of them.
func TestMoBuiltinsReportBadArgsAsSQLError(t *testing.T) {
	si := &starlarkInterpreter{}
	notAString := starlark.Tuple{starlark.MakeInt(1)}

	for _, c := range []struct {
		name string
		fn   func(*starlark.Thread, *starlark.Builtin, starlark.Tuple, []starlark.Tuple) (starlark.Value, error)
		args starlark.Tuple
	}{
		{"mo.sql", si.moSql, notAString},
		{"mo.jq", si.moJq, notAString}, // also too few arguments
		{"mo.quote", si.moQuote, notAString},
		{"mo.getvar", si.moGetVar, notAString},
		{"mo.setvar", si.moSetVar, notAString}, // also too few arguments
		{"mo.llm_connect", si.moLlmConnect, notAString},
		{"mo.llm_chat", si.moLlmChat, notAString},
	} {
		v, err := c.fn(nil, starlark.NewBuiltin(c.name, c.fn), c.args, nil)
		require.NoError(t, err, "%s must not abort the script", c.name)

		list, ok := v.(*starlark.List)
		require.True(t, ok, "%s must return the [result, ok] pair", c.name)
		require.Equal(t, 2, list.Len())
		require.Equal(t, starlark.None, list.Index(0), "%s: no result on failure", c.name)

		e, ok := list.Index(1).(*sqlError)
		require.True(t, ok, "%s must report the failure as an mo.error", c.name)
		require.NotEmpty(t, e.message)
		// argument unpacking is not a SQL failure, so there is no code to
		// pretend to: a procedure branching on err.code sees 0, not a
		// borrowed error number
		require.Equal(t, uint16(0), e.code, "%s", c.name)
		require.Equal(t, "", e.sqlstate, "%s", c.name)
	}
}
