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
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
	"testing"
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
	for _, name := range []string{"utf8_general_ci", "utf8mb3_bin", "utf8mb4_0900_ai_ci"} {
		require.NoError(t, ses.SetSessionSysVar(t.Context(), "collation_connection", name))
	}
	require.Error(t, ses.SetSessionSysVar(t.Context(), "default_collation_for_utf8mb4", "utf8mb4_0900_bin"))
	require.Error(t, validateCharsetSystemVariable("default_collation_for_utf8mb4", "utf8mb4_0900_bin"))
	require.NoError(t, ses.SetSessionSysVar(t.Context(), "character_set_results", nil))
	require.NoError(t, ses.SetSessionSysVar(t.Context(), "collation_connection", "default"))
	require.NoError(t, validateCharsetSystemVariable("unrelated_variable", nil))
	require.Error(t, validateCharsetSystemVariable("character_set_client", 123))
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
		} else {
			require.ErrorContains(t, err, "unsupported handshake character set", "ID=%d", id)
			require.Equal(t, 45, proto.collationID)
			require.Equal(t, []byte("unchanged"), proto.authResponse)
			require.Equal(t, capabilities, proto.capability)
		}
	}
}

func TestCharsetAdmissionChangeUserProtocol(t *testing.T) {
	for _, tc := range []struct {
		id       int
		admitted bool
	}{
		{33, true}, {45, true}, {46, true}, {63, true}, {83, true}, {255, true},
		{8, false}, {47, false}, {65, false}, {28, false}, {87, false}, {192, false}, {224, false}, {309, false}, {65535, false},
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
