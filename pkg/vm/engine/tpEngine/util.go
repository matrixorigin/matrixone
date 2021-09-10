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

package tpEngine

import "fmt"

func string2bytes(s string)[]byte{
	return []byte(s)
}

func bytes2string(bts []byte)string{
	return string(bts)
}

/**
check if x in a slice
*/
func isInSlice(x string,arr []string) bool {
	for _,y := range arr{
		if x == y {
			return true
		}
	}
	return false
}

//remove a string from array, return a new slice
func removeFromSlice(x string,arr []string)[]string {
	var i int
	var y string
	for i,y = range arr{
		if x == y {
			break
		}
	}

	if i < len(arr) {
		return append(arr[:i],arr[i+1:]...)
	}
	return arr
}

/**
make a byte slice with designated value
*/
func makeByteSlice(l int,v byte)[]byte{
	u := make([]byte,l)
	for i := 0; i < l; i += 1 {
		u[i] = v
	}
	return u
}

func sKey(num int, id string) string {
	return fmt.Sprintf("%v.%v", id, num)
}