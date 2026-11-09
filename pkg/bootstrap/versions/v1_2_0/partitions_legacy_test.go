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

package v1_2_0

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartitionsUpgradeKeepsReleasedDefinition(t *testing.T) {
	// SHA-256 of InformationSchemaPartitionsDDL evaluated from the published
	// v4.2.8 source at 10d204b8763f19b2b37ba2147d1d4c1a7828d6d4.
	digest := sha256.Sum256([]byte(upg_information_schema_partitions.UpgSql))
	require.Equal(t, "5bef2546566ddf69d36e0cdffe281be1de3bad3672005b6ecaec623564261f17", fmt.Sprintf("%x", digest))
}
