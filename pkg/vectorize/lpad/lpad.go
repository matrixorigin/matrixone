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

func lpadOne(a string, b0 int, c string) string {
	c0 := len(c)
	if c0 == 0 {
		panic("Pad with empty string")
	}
	if len(a) > b0 {
		return string([]byte(a)[:b0])
	} else {
		lens := b0 - len(a)
		t1 := lens / c0
		t2 := lens % c0
		tmps := []byte{}
		for j := 0; j < t1; j++ {
			tmps = append(tmps, c...)
		}
		tmps = append(tmps, []byte(c)[:t2]...)
		tmps = append(tmps, a...)
		return string(tmps)
	}
}

func LpadVarchar(a []string, b []int64, c []string) []string {
	// XXX This function, always take b[0] and c[0], is this correct?
	b0 := int(b[0])
	c0 := c[0]
	res := make([]string, len(a))
	//in fact,the length of three slice is the same with each other
	for i := range a {
		res[i] = lpadOne(a[i], b0, c0)
	}
	return res
}

func Lpad(res []string, src []string, length uint32, pad []string) []string {
	for idx, str := range src {
		res[idx] = lpadOne(str, int(length), pad[idx])
	}
	return res
}
