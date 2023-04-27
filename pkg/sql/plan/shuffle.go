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

package plan

func SimpleHashToRange(bytes []byte, upperLimit int) int {
	lenBytes := len(bytes)
	//sample five bytes
	return (int(bytes[0])*(int(bytes[lenBytes/4])+int(bytes[lenBytes/2])+int(bytes[lenBytes*3/4])) + int(bytes[lenBytes-1])) % upperLimit
}
