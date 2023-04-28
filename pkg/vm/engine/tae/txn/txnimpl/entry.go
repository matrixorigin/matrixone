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

package txnimpl

import (
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
)

const (
	IOET_WALEntry_TxnRecord = entry.IOET_WALEntry_CustomizedStart + iota
	IOET_WALEntry_TxnState
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALEntry_TxnRecord,
			Version: entry.IOET_WALEntry_V1,
		}, nil, entry.UnmarshalEntry,
	)
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALEntry_TxnState,
			Version: entry.IOET_WALEntry_V1,
		}, nil, entry.UnmarshalEntry,
	)
}
