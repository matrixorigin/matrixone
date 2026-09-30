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

package proxy

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/frontend"
)

func TestWriteInitialHandshakeRejectsInvalidSalt(t *testing.T) {
	proto := frontend.NewMysqlClientProtocol("", 1, nil, 0, &config.FrontendParameters{})
	proto.SetSalt(nil)
	client := &clientConn{mysqlProto: proto}
	require.ErrorContains(t, client.writeInitialHandshake(), "invalid handshake salt length")
}
