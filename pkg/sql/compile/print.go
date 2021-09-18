// Copyright 2021 Matrix Origin
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

package compile

import (
	"fmt"
	"matrixone/pkg/vm/pipeline"
)

func Print(prefix []byte, ss []*Scope) {
	for _, s := range ss {
		if s.Magic == Merge || s.Magic == Remote {
			Print(append(prefix, '\t'), s.PreScopes)
		}
		p := pipeline.NewMerge(s.Instructions)
		fmt.Printf("%s:%v %v\n", prefix, s.Magic, p)
	}
}
