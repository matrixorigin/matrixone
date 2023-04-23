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

package api

func (m *SyncLogTailReq) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *SyncLogTailReq) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m *SyncLogTailResp) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *SyncLogTailResp) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}

func (m *PrecommitWriteCmd) MarshalBinary() ([]byte, error) {
	return m.Marshal()
}

func (m *PrecommitWriteCmd) UnmarshalBinary(data []byte) error {
	return m.Unmarshal(data)
}
