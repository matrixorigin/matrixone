// Copyright 2022 Matrix Origin
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

package catalog

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIndexBuildParamsRoundTrip covers the promoted build params
// (kmeans_train_percent, kmeans_max_iteration, max_index_capacity) as
// first-class flat algo_params keys: SHOW CREATE rendering and the flat-map
// read-back.
func TestIndexBuildParamsRoundTrip(t *testing.T) {
	algoParams := `{"lists":"8","op_type":"vector_l2_ops","kmeans_train_percent":"5","kmeans_max_iteration":"30","max_index_capacity":"2000"}`

	// SHOW CREATE / restore DDL rendering must emit the new keys in a
	// re-parseable form ("<key> = <val>").
	s, err := IndexParamsToStringList(algoParams)
	require.Nil(t, err)
	require.True(t, strings.Contains(s, IndexAlgoParamKmeansTrainPercent+" = 5"), s)
	require.True(t, strings.Contains(s, IndexAlgoParamKmeansMaxIteration+" = 30"), s)
	require.True(t, strings.Contains(s, IndexAlgoParamMaxIndexCapacity+" = 2000"), s)

	// Flat-map read-back (what the build path consumes).
	m, err := IndexParamsStringToMap(algoParams)
	require.Nil(t, err)
	require.Equal(t, "5", m[IndexAlgoParamKmeansTrainPercent])
	require.Equal(t, "30", m[IndexAlgoParamKmeansMaxIteration])
	require.Equal(t, "2000", m[IndexAlgoParamMaxIndexCapacity])

	// Absent keys (legacy index) round-trip cleanly with no rendering.
	s, err = IndexParamsToStringList(`{"lists":"8"}`)
	require.Nil(t, err)
	require.False(t, strings.Contains(s, IndexAlgoParamKmeansTrainPercent), s)
	require.False(t, strings.Contains(s, IndexAlgoParamMaxIndexCapacity), s)
}

func TestIsIndexAsync(t *testing.T) {

	var (
		json string
		err  error
		ok   bool
	)

	json = ""
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, false)

	json = "{}"
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, false)

	json = `{"async": "true"}`
	ok, err = IsIndexAsync(json)
	require.Nil(t, err)
	require.Equal(t, ok, true)

	json = `{"async": 1}`
	_, err = IsIndexAsync(json)
	require.NotNil(t, err)
}
