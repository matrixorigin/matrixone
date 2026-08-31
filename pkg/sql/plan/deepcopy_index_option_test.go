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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestDeepCopyIndexOptionPreservesFullTextParser(t *testing.T) {
	original := &planpb.IndexOption{
		CreateExtraTable: true,
		ParserName:       "gojieba",
		NgramTokenSize:   3,
	}

	copy := DeepCopyIndexOption(original)
	require.Equal(t, original, copy)

	copy.ParserName = "ngram"
	copy.NgramTokenSize = 2
	require.Equal(t, "gojieba", original.ParserName)
	require.Equal(t, int32(3), original.NgramTokenSize)
}

func TestDeepCopyIndexOptionNil(t *testing.T) {
	require.Nil(t, DeepCopyIndexOption(nil))
}
