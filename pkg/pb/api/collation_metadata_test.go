// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"
)

func TestSchemaExtraCollationRoundTrip(t *testing.T) {
	for _, extra := range []*SchemaExtra{{}, {DefaultCharset: 3},
		{DefaultCharset: 4, CollationVersion: 1, KeyFormat: 1, AutoIdCache: 7, XXX_unrecognized: []byte{0x98, 0x06, 0x07}}} {
		wire, err := extra.Marshal()
		require.NoError(t, err)
		var restored SchemaExtra
		require.NoError(t, restored.Unmarshal(wire))
		require.True(t, proto.Equal(extra, &restored))
		require.True(t, proto.Equal(extra, CloneExtra(&restored)))
	}
	for _, extra := range []*SchemaExtra{{DefaultCharset: 259}, {CollationVersion: 2}, {KeyFormat: 2}} {
		wire, err := extra.Marshal()
		require.NoError(t, err)
		var restored SchemaExtra
		require.Error(t, restored.Unmarshal(wire))
	}
}
