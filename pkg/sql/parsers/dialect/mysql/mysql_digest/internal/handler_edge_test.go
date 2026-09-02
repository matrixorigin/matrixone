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

package internal

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func runHandler(sql string, mode SQLMode, maxLength int, rejectMarkers bool) (*Lexer, *tokenHandler, error) {
	lexer := NewLexer(sql)
	lexer.SetSQLMode(mode)
	lexer.SetPrepareMode(rejectMarkers)
	store := NewTokenStore(&maxLength)
	handler := NewTokenHandler(lexer, store, NewReducer(store))
	handler.SetRejectParameterMarkers(rejectMarkers)
	return lexer, handler, handler.ProcessAll()
}

func TestHandlerContinuesLexicalValidationAfterBufferFull(t *testing.T) {
	lexer, handler, err := runHandler("SELECT 'unterminated", 0, 0, false)
	require.ErrorContains(t, err, ErrUnterminatedString)
	require.True(t, handler.SawToken())
	require.False(t, lexer.SawComment())
}

func TestHandlerRejectsParameterMarkerOnlyWhenRequested(t *testing.T) {
	_, _, err := runHandler("SELECT ?", 0, 1024, true)
	require.ErrorContains(t, err, ErrParameterMarker)

	lexer, handler, err := runHandler("SELECT '?' /* ? */", 0, 1024, true)
	require.NoError(t, err)
	require.True(t, handler.SawToken())
	require.False(t, lexer.SawComment() && !handler.SawToken())
}

func TestLexerCommentClassificationAndSQLModes(t *testing.T) {
	lexer, handler, err := runHandler("/* comment */", 0, 0, false)
	require.NoError(t, err)
	require.True(t, lexer.SawComment())
	require.False(t, lexer.SawNonComment())
	require.False(t, handler.SawToken())

	lexer, handler, err = runHandler("/* comment */\x00SELECT 1", 0, 0, false)
	require.NoError(t, err)
	require.True(t, lexer.SawComment())
	require.True(t, lexer.SawNonComment())
	require.False(t, handler.SawToken())

	_, handler, err = runHandler("SELECT 1 || 0", 0, 1024, false)
	require.NoError(t, err)
	defaultText := handler.store.BuildText()
	_, handler, err = runHandler("SELECT 1 || 0", MODE_PIPES_AS_CONCAT, 1024, false)
	require.NoError(t, err)
	require.Equal(t, defaultText, handler.store.BuildText())

	_, handler, err = runHandler("SELECT NOT 1", MODE_HIGH_NOT_PRECEDENCE, 1024, false)
	require.NoError(t, err)
	require.Contains(t, handler.store.BuildText(), "!")
	_, handler, err = runHandler("SELECT a IS NOT NULL", MODE_HIGH_NOT_PRECEDENCE, 1024, false)
	require.NoError(t, err)
	require.Contains(t, handler.store.BuildText(), "NULL")
}

func TestLexerNCharAndDollarErrors(t *testing.T) {
	_, _, err := runHandler(`SELECT N'a\'b'`, 0, 1024, false)
	require.NoError(t, err)
	_, _, err = runHandler(`SELECT N'a\'b'`, MODE_NO_BACKSLASH_ESCAPES, 1024, false)
	require.Error(t, err)
	_, _, err = runHandler("SELECT $$unterminated", 0, 1024, false)
	require.ErrorContains(t, err, ErrUnterminatedDollar)
}
