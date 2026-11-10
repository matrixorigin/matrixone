// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"encoding/binary"
	"strconv"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

func TestCharsetAdmissionParsedRequests(t *testing.T) {
	for _, tc := range []struct{ sql, value, errorText string }{
		{"set names latin1", "latin1", "unsupported character set"},
		{"set names utf32", "utf32", "unsupported character set"},
		{"set charset utf32", "utf32", "unsupported character set"},
		{"set names utf8mb4 collate utf32_bin", "utf8mb4", "unsupported collation"},
		{"set names utf8mb4 collate utf32_general_ci", "utf8mb4", "unsupported collation"},
		{"set character set latin1", "latin1", "unsupported character set"},
		{"set charset latin1", "latin1", "unsupported character set"},
		{"set char set latin1", "latin1", "unsupported character set"},
		{"set names utf8 collate latin1_bin", "utf8", "unsupported collation"},
		{"set names utf8 collate 'binary'", "utf8", "is not valid for CHARACTER SET"},
		{"set names utf8mb3 collate utf8_bin", "utf8mb3", ""},
		{"set names utf8mb4 collate utf8mb4_0900_ai_ci", "utf8mb4", ""},
		{"set names utf8 collate utf8_unicode_ci", "utf8", ""},
		{"set names utf8mb4 collate utf8mb4_unicode_ci", "utf8mb4", ""},
		{"set character set utf8mb3", "utf8mb3", ""},
		{"set names default", "default", ""},
		{"set charset default", "default", ""},
		{"set @charset = 'latin1'", "latin1", ""},
		{"set @character = 'latin1'", "latin1", ""},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			assign := stmt.(*tree.SetVar).Assignments[0]
			err = validateCharsetAssignment(assign, tc.value)
			if tc.errorText == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.errorText)
			}
		})
	}
	require.Error(t, validateCharsetAssignment(&tree.VarAssignmentExpr{SetNames: true, Reserved: &tree.UnresolvedName{}}, "utf8"))
}

func TestCharsetAdmissionBeforeSessionOrGlobalMutation(t *testing.T) {
	ses := &Session{}
	for name, bad := range map[string]string{
		"character_set_client": "latin1", "character_set_connection": "latin1", "character_set_results": "latin1", "character_set_server": "latin1",
		"collation_connection": "latin1_bin", "collation_server": "latin1_swedish_ci",
	} {
		t.Run(name, func(t *testing.T) {
			before, err := ses.GetSessionSysVar(name)
			require.NoError(t, err)
			if name == "collation_server" {
				require.ErrorContains(t, ses.SetSessionSysVar(t.Context(), name, bad), "global")
			} else {
				require.ErrorContains(t, ses.SetSessionSysVar(t.Context(), name, bad), "unsupported")
			}
			after, err := ses.GetSessionSysVar(name)
			require.NoError(t, err)
			require.Equal(t, before, after)
			// A nil global store / persistence backend is intentional: rejection must
			// precede both the global catalog write and publication to the live cache.
			require.ErrorContains(t, ses.SetGlobalSysVar(t.Context(), name, bad), "unsupported")
			require.Nil(t, ses.gSysVars)
		})
	}
	for _, name := range []string{"utf8", "utf8mb3", "UTF8MB4", "binary"} {
		require.NoError(t, ses.SetSessionSysVar(t.Context(), "CHARACTER_SET_CONNECTION", name))
		got, err := ses.GetSessionSysVar("character_set_connection")
		require.NoError(t, err)
		require.Equal(t, name, got)
	}
	for _, name := range []string{"utf8_general_ci", "utf8mb3_bin", "utf8mb4_0900_ai_ci", "utf8_unicode_ci", "utf8mb4_unicode_ci"} {
		require.NoError(t, ses.SetSessionSysVar(t.Context(), "collation_connection", name))
		got, err := ses.GetSessionSysVar("collation_connection")
		require.NoError(t, err)
		require.Equal(t, name, got)
	}
	require.Error(t, ses.SetSessionSysVar(t.Context(), "default_collation_for_utf8mb4", "utf8mb4_0900_bin"))
	require.Error(t, validateCharsetSystemVariable("default_collation_for_utf8mb4", "utf8mb4_0900_bin"))
	require.NoError(t, ses.SetSessionSysVar(t.Context(), "character_set_results", nil))
	require.NoError(t, ses.SetSessionSysVar(t.Context(), "collation_connection", "default"))
	require.NoError(t, validateCharsetSystemVariable("unrelated_variable", nil))
	require.Error(t, validateCharsetSystemVariable("character_set_client", 123))
}

func TestSetNamesCollationUpdatesConnectionCollation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := defines.AttachAccountId(t.Context(), sysAccountID)

	for _, tc := range []struct {
		sql         string
		wantCharset string
		wantName    string
	}{
		{sql: "set names utf8 collate utf8_unicode_ci", wantCharset: "utf8", wantName: "utf8_unicode_ci"},
		{sql: "set names utf8mb4 collate utf8mb4_unicode_ci", wantCharset: "utf8mb4", wantName: "utf8mb4_unicode_ci"},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			charsetVars := []string{
				"character_set_client", "character_set_connection", "character_set_results",
			}
			for _, name := range charsetVars {
				require.NoError(t, ses.SetSessionSysVar(t.Context(), name, "binary"))
				require.NoError(t, ses.SetUserDefinedVar(name, "user-before", t.Name()))
			}
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			execCtx := newTestExecCtx(ctx, ctrl)
			execCtx.ses = ses
			execCtx.singleStatementQuery = true
			require.NoError(t, doSetVar(ses, execCtx, stmt.(*tree.SetVar), tc.sql, false))
			for _, name := range charsetVars {
				got, err := ses.GetSessionSysVar(name)
				require.NoError(t, err)
				require.Equal(t, tc.wantCharset, got)

				userVar, err := ses.GetUserDefinedVar(name)
				require.NoError(t, err)
				require.Equal(t, "user-before", userVar.Value)
			}
			got, err := ses.GetSessionSysVar("collation_connection")
			require.NoError(t, err)
			require.Equal(t, tc.wantName, got)
		})
	}
}

func TestSetNamesDefaultCompatibility(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := defines.AttachAccountId(t.Context(), sysAccountID)

	// Start from a valid native-UCA connection setup. A subsequent plain
	// SET NAMES DEFAULT must reset the implicit connection collation as well as
	// the three character-set variables.
	setupStmt, err := parsers.ParseOne(ctx, dialect.MYSQL,
		"set names utf8 collate utf8_unicode_ci", 1)
	require.NoError(t, err)
	setupExecCtx := newTestExecCtx(ctx, ctrl)
	setupExecCtx.ses = ses
	setupExecCtx.singleStatementQuery = true
	require.NoError(t, doSetVar(ses, setupExecCtx, setupStmt.(*tree.SetVar),
		"set names utf8 collate utf8_unicode_ci", false))
	setupStmt.Free()
	setupCollation, err := ses.GetSessionSysVar("collation_connection")
	require.NoError(t, err)
	require.Equal(t, "utf8_unicode_ci", setupCollation)

	charsetVars := []string{
		"character_set_client", "character_set_connection", "character_set_results",
	}
	for _, name := range charsetVars {
		require.NoError(t, ses.SetUserDefinedVar(name, "user-before", t.Name()))
	}

	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "set names default", 1)
	require.NoError(t, err)
	defer stmt.Free()
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.singleStatementQuery = true
	require.NoError(t, doSetVar(ses, execCtx, stmt.(*tree.SetVar), "set names default", false))

	wantCharset, err := ses.GetGlobalSysVar("character_set_server")
	require.NoError(t, err)
	if wantCharset == nil {
		wantCharset = gSysVarsDefs["character_set_server"].Default
	}
	for _, name := range charsetVars {
		got, err := ses.GetSessionSysVar(name)
		require.NoError(t, err)
		require.Equal(t, wantCharset, got)

		userVar, err := ses.GetUserDefinedVar(name)
		require.NoError(t, err)
		require.Equal(t, "user-before", userVar.Value)
	}
	wantCollation, err := ses.GetGlobalSysVar("collation_server")
	require.NoError(t, err)
	got, err := ses.GetSessionSysVar("collation_connection")
	require.NoError(t, err)
	require.Equal(t, wantCollation, got)
}

func TestSetNamesDefaultRejectsIncompatibleServerDefaults(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := defines.AttachAccountId(t.Context(), sysAccountID)

	// The two server defaults have independent catalog entries. Simulate an
	// old/manual catalog containing a collation from a different charset and
	// ensure SET NAMES DEFAULT rejects it before changing session state.
	globalVars := ses.gSysVars.Clone()
	globalVars.Set("character_set_server", "utf8mb4")
	globalVars.Set("collation_server", "utf8_unicode_ci")
	ses.gSysVars = globalVars
	ses.sesSysVars = globalVars.Clone()
	for _, name := range []string{
		"character_set_client", "character_set_connection", "character_set_results",
	} {
		require.NoError(t, ses.SetSessionSysVar(ctx, name, "binary"))
	}
	require.NoError(t, ses.SetSessionSysVar(ctx, "collation_connection", "binary"))

	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "set names default", 1)
	require.NoError(t, err)
	defer stmt.Free()
	execCtx := newTestExecCtx(ctx, ctrl)
	execCtx.ses = ses
	execCtx.singleStatementQuery = true
	err = doSetVar(ses, execCtx, stmt.(*tree.SetVar), "set names default", false)
	require.ErrorContains(t, err, "not valid for CHARACTER SET")

	for _, name := range []string{
		"character_set_client", "character_set_connection", "character_set_results",
	} {
		got, getErr := ses.GetSessionSysVar(name)
		require.NoError(t, getErr)
		require.Equal(t, "binary", got)
	}
	got, err := ses.GetSessionSysVar("collation_connection")
	require.NoError(t, err)
	require.Equal(t, "binary", got)
}

func TestValidateCharsetCollationPair(t *testing.T) {
	for _, tc := range []struct {
		name      string
		charset   interface{}
		collation string
		err       string
	}{
		{name: "non-string charset", charset: 1, collation: "utf8mb4_general_ci", err: "character set must be a string"},
		{name: "unknown charset", charset: "latin1", collation: "latin1_swedish_ci", err: "unsupported character set"},
		{name: "unknown collation", charset: "utf8mb4", collation: "utf8mb4_0900_bin", err: "unsupported collation"},
		{name: "native mismatch", charset: "utf8mb4", collation: "utf8_unicode_ci", err: "not valid for CHARACTER SET"},
		{name: "native utf8 pair", charset: "utf8", collation: "utf8_unicode_ci"},
		{name: "legacy utf8mb4 pair", charset: "utf8mb4", collation: "utf8mb4_general_ci"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCharsetCollationPair(tc.charset, tc.collation)
			if tc.err == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tc.err)
			}
		})
	}
}

func TestSetNamesImplicitCollation(t *testing.T) {
	ctrl := gomock.NewController(t)
	ses := newTestSession(t, ctrl)
	defer ses.Close()
	ctx := defines.AttachAccountId(t.Context(), sysAccountID)

	wantUTF8MB4, err := ses.GetSessionSysVar("default_collation_for_utf8mb4")
	require.NoError(t, err)
	if wantUTF8MB4 == nil {
		wantUTF8MB4 = gSysVarsDefs["default_collation_for_utf8mb4"].Default
	}

	cases := []struct {
		sql         string
		wantCharset string
		wantName    string
	}{
		{sql: "set names utf8mb4", wantCharset: "utf8mb4", wantName: wantUTF8MB4.(string)},
		{sql: "set names utf8", wantCharset: "utf8", wantName: "utf8_general_ci"},
		{sql: "set names binary", wantCharset: "binary", wantName: "binary"},
	}

	for _, tc := range cases {
		for _, prepared := range []bool{false, true} {
			t.Run(tc.sql+"/prepared="+strconv.FormatBool(prepared), func(t *testing.T) {
				setup, setupErr := parsers.ParseOne(ctx, dialect.MYSQL,
					"set names utf8 collate utf8_unicode_ci", 1)
				require.NoError(t, setupErr)
				setupExecCtx := newTestExecCtx(ctx, ctrl)
				setupExecCtx.ses = ses
				setupExecCtx.singleStatementQuery = true
				require.NoError(t, doSetVar(ses, setupExecCtx, setup.(*tree.SetVar),
					"set names utf8 collate utf8_unicode_ci", prepared))
				setup.Free()

				stmt, parseErr := parsers.ParseOne(ctx, dialect.MYSQL, tc.sql, 1)
				require.NoError(t, parseErr)
				defer stmt.Free()
				execCtx := newTestExecCtx(ctx, ctrl)
				execCtx.ses = ses
				execCtx.singleStatementQuery = true
				require.NoError(t, doSetVar(ses, execCtx, stmt.(*tree.SetVar), tc.sql, prepared))
				for _, name := range []string{
					"character_set_client", "character_set_connection", "character_set_results",
				} {
					got, getErr := ses.GetSessionSysVar(name)
					require.NoError(t, getErr)
					require.Equal(t, tc.wantCharset, got)
				}
				got, getErr := ses.GetSessionSysVar("collation_connection")
				require.NoError(t, getErr)
				require.Equal(t, tc.wantName, got)
			})
		}
	}
}

func TestResolveImplicitSetNamesCollationRejectsInvalidRequests(t *testing.T) {
	ses := &Session{}

	_, err := resolveImplicitSetNamesCollation(ses, 123)
	require.ErrorContains(t, err, "character set must be a string")

	_, err = resolveImplicitSetNamesCollation(ses, "latin1")
	require.ErrorContains(t, err, "unsupported character set")
}

func TestCharsetAdmissionHandshake(t *testing.T) {
	for _, id := range []byte{8, 47, 65, 192, 224, 33, 45, 46, 63, 83, 255} {
		capabilities := uint32(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION)
		proto := &MysqlProtocolImpl{io: gIO, capability: capabilities, collationID: 45, charset: "utf8mb4", authResponse: []byte("unchanged")}
		payload := make([]byte, 32, 35)
		binary.LittleEndian.PutUint32(payload, capabilities)
		payload[8] = id
		payload = append(payload, 'u', 0, 0)
		_, err := proto.HandleHandshake(t.Context(), payload)
		_, admitted := lookupSupportedProtocolCollation(int(id))
		if admitted {
			require.NoError(t, err, "ID=%d", id)
			require.Equal(t, int(id), proto.collationID)
			name, ok := lookupSupportedProtocolCollation(int(id))
			require.True(t, ok)
			require.Equal(t, name.collationName, proto.collationName)
			require.Equal(t, name.charset, proto.charset)
		} else {
			require.ErrorContains(t, err, "unsupported handshake character set", "ID=%d", id)
			require.Equal(t, 45, proto.collationID)
			require.Equal(t, []byte("unchanged"), proto.authResponse)
			require.Equal(t, capabilities, proto.capability)
		}
	}
}

func TestNegotiatedSessionCharsetInitializesSessionVariables(t *testing.T) {
	for _, tc := range []struct {
		name      string
		charset   string
		collation string
	}{
		{name: "utf8mb4 general", charset: "utf8mb4", collation: "utf8mb4_general_ci"},
		{name: "native utf8", charset: "utf8", collation: "utf8_unicode_ci"},
		{name: "native utf8mb4", charset: "utf8mb4", collation: "utf8mb4_unicode_ci"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ses := &Session{}
			proto := &MysqlProtocolImpl{
				ses:           ses,
				charset:       tc.charset,
				collationName: tc.collation,
			}

			require.NoError(t, proto.applyNegotiatedSessionCharset(t.Context()))
			for _, name := range []string{
				"character_set_client",
				"character_set_connection",
				"character_set_results",
			} {
				got, err := ses.GetSessionSysVar(name)
				require.NoError(t, err)
				require.Equal(t, tc.charset, got, name)
			}
			got, err := ses.GetSessionSysVar("collation_connection")
			require.NoError(t, err)
			require.Equal(t, tc.collation, got)
		})
	}
}

func TestNegotiatedSessionCharsetRejectsIncompleteState(t *testing.T) {
	// A protocol object can reach the publication boundary before handshake
	// negotiation (for example, on an auth-switch path).  An incomplete pair
	// must be a no-op rather than mutating the session.
	empty := &MysqlProtocolImpl{}
	require.NoError(t, empty.applyNegotiatedSessionCharsetTo(t.Context(), &Session{}))

	invalid := &MysqlProtocolImpl{
		charset:       "utf8mb4",
		collationName: "utf8_unicode_ci",
	}
	require.ErrorContains(t,
		invalid.applyNegotiatedSessionCharsetTo(t.Context(), &Session{}),
		"not valid for CHARACTER SET")

	valid := &MysqlProtocolImpl{
		charset:       "utf8mb4",
		collationName: "utf8mb4_unicode_ci",
	}
	require.ErrorContains(t,
		valid.applyNegotiatedSessionCharsetTo(t.Context(), nil),
		"cannot initialize negotiated charset without a session")
}

func TestCharsetAdmissionChangeUserProtocol(t *testing.T) {
	for _, tc := range []struct {
		id       int
		admitted bool
	}{
		{33, true}, {45, true}, {46, true}, {63, true}, {83, true}, {255, true},
		{8, false}, {47, false}, {65, false}, {28, false}, {87, false}, {192, true}, {224, true}, {309, false}, {65535, false},
	} {
		proto := &MysqlProtocolImpl{io: gIO, capability: CLIENT_SECURE_CONNECTION | CLIENT_PROTOCOL_41}
		// username, empty secure auth, empty database, full uint16 protocol ID.
		payload := []byte{'u', 0, 0, 0, byte(tc.id), byte(tc.id >> 8)}
		req, err := proto.parseChangeUserRequest(t.Context(), payload)
		require.Equal(t, tc.id, req.collationID)
		if tc.admitted {
			require.NoError(t, err, "ID=%d", tc.id)
			require.Equal(t, tc.id, req.collationID)
			name, ok := lookupSupportedProtocolCollation(tc.id)
			require.True(t, ok)
			require.NotEmpty(t, name.collationName)
		} else {
			require.ErrorContains(t, err, "unsupported COM_CHANGE_USER character set", "ID=%d", tc.id)
		}
	}
	for _, id := range []int{-1, 65536} {
		_, ok := lookupSupportedProtocolCollation(id)
		require.False(t, ok)
	}
}
