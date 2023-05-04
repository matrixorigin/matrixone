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

package txnentries

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
)

const (
	IOET_WALTxnCommand_Compact uint16 = 3006
	IOET_WALTxnCommand_Merge   uint16 = 3007

	IOET_WALTxnCommand_Compact_V1 uint16 = 1
	IOET_WALTxnCommand_Merge_V1   uint16 = 1

	IOET_WALTxnCommand_Compact_CurrVer = IOET_WALTxnCommand_Compact_V1
	IOET_WALTxnCommand_Merge_CurrVer   = IOET_WALTxnCommand_Merge_V1
)

func init() {

	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Compact,
			Version: IOET_WALTxnCommand_Compact_V1,
		},
		nil, func(b []byte) (any, error) {
			cmd := new(compactBlockCmd)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALTxnCommand_Merge,
			Version: IOET_WALTxnCommand_Merge_V1,
		},
		nil,
		func(b []byte) (any, error) {
			cmd := new(mergeBlocksCmd)
			err := cmd.UnmarshalBinary(b)
			return cmd, err
		},
	)
}
