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

package ltrim

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLtrim(t *testing.T) {
	multiStrings := []string{
		"",
		" ",
		"  ",
		"   ",
		" a",
		" a ",
		" a  ",
		"  a ",
		" 你好 ",
		"　a", // fullwidth space
	}
	rs := make([]string, len(multiStrings))
	Ltrim(multiStrings, rs)
	for i, s := range multiStrings {
		// TrimLeft of only space " " char.
		require.Equal(t, rs[i], strings.TrimLeft(s, " \u3000"))
	}
}
