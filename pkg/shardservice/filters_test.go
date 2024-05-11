// Copyright 2021-2024 Matrix Origin
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

package shardservice

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFreezeFilter(t *testing.T) {
	cns := make([]*cn, 0, 2)
	cns = append(cns, &cn{id: "cn1"}, &cn{id: "cn2"})

	timeout := time.Minute
	f := newFreezeFilter(timeout)
	f.freeze["cn2"] = time.Now()
	f.freeze["cn1"] = time.Now().Add(-timeout)
	cns = f.filter(nil, cns)
	require.Equal(t, 1, len(cns))
	require.Equal(t, "cn1", cns[0].id)
}
