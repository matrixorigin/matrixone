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

package vectorindex

import (
	"encoding/json"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDecodeMaintenanceConfig(t *testing.T) {
	cfg, err := DecodeMaintenanceConfig(`{"db":"db","src":"source","metadata":"meta","index":"store","ignored":"ignored"}`)
	require.NoError(t, err)
	require.Equal(t, IndexTableConfig{DbName: "db", SrcTable: "source", MetadataTable: "meta", IndexTable: "store", IgnoredTable: "ignored"}, cfg)
	for _, raw := range []string{"null", "{}", `{"src":""}`} {
		_, err := DecodeMaintenanceConfig(raw)
		require.True(t, moerr.IsMoErrCode(err, moerr.ErrInvalidInput))
	}
	for _, raw := range []string{"", "{", `{"src":1}`} {
		var expected IndexTableConfig
		want := json.Unmarshal([]byte(raw), &expected)
		_, got := DecodeMaintenanceConfig(raw)
		require.IsType(t, want, got)
		require.EqualError(t, got, want.Error())
	}
}
