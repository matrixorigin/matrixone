// Copyright 2023 Matrix Origin
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

package goroutine

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseGoroutineLine(t *testing.T) {
	cases := []struct {
		line   string
		expect Goroutine
	}{
		{
			line: "goroutine 628 [IO wait, 8 minutes]:",
			expect: Goroutine{
				ID:             628,
				State:          "IO wait",
				BlockedMinutes: 8,
				rawState:       "goroutine 628 [IO wait, 8 minutes]:",
			},
		},
		{
			line: "goroutine 628 [runnable]:",
			expect: Goroutine{
				ID:       628,
				State:    "runnable",
				rawState: "goroutine 628 [runnable]:",
			},
		},
	}

	for _, c := range cases {
		g := Goroutine{}
		readGoroutineLine(c.line, &g)
		assert.Equal(t, c.expect, g)
	}
}

func TestParse(t *testing.T) {
	stacks := `goroutine 99 [select]:
	github.com/panjf2000/ants/v2.(*Pool).purgeStaleWorkers(0xc000504000, {0x36add70, 0xc0007b5f90})
		/home/go/pkg/mod/github.com/panjf2000/ants/v2@v2.7.4/pool.go:83 +0x10a
	created by github.com/panjf2000/ants/v2.(*Pool).goPurge
		/home/go/pkg/mod/github.com/panjf2000/ants/v2@v2.7.4/pool.go:148 +0xe5
	
	goroutine 100 [select]:
	github.com/panjf2000/ants/v2.(*Pool).ticktock(0xc000504000, {0x36add70, 0xc000054140})
		/home/go/pkg/mod/github.com/panjf2000/ants/v2@v2.7.4/pool.go:126 +0x145
	created by github.com/panjf2000/ants/v2.(*Pool).goTicktock
		/home/go/pkg/mod/github.com/panjf2000/ants/v2@v2.7.4/pool.go:155 +0x115
		`
	gs := parse([]byte(stacks))
	require.Equal(t, 2, len(gs))
}
