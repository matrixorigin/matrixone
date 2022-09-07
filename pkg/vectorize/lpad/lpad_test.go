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

package lpad

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLpadVarchar(t *testing.T) {
	origins := []string{"hi", "hi", "hishjajsa",
		"hish&sa*(#jsa", "hish&sa*(#jsa", "hishjajsa", "abc"}
	originsInt64 := []int64{22}

	originsPadd := []string{"??so"}

	//Predefined Correct Values
	results := []string{"??so??so??so??so??sohi", "??so??so??so??so??sohi",
		"??so??so??so?hishjajsa", "??so??so?hish&sa*(#jsa",
		"??so??so?hish&sa*(#jsa", "??so??so??so?hishjajsa",
		"??so??so??so??so??sabc"}

	or := LpadVarchar(origins, originsInt64, originsPadd)

	for i, r := range results {
		require.Equal(t, or[i], r)
	}
}
