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

package logservicedriver

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
)

const (
	IOET_WALRecord_V1 uint16 = 1
	IOET_WALRecord_V2 uint16 = 2
	IOET_WALRecord    uint16 = 1000

	IOET_WALRecord_CurrVer = IOET_WALRecord_V2
)

func init() {
	objectio.RegisterIOEnrtyCodec(
		objectio.IOEntryHeader{
			Type:    IOET_WALRecord,
			Version: IOET_WALRecord_V1,
		},
		func(a any) ([]byte, error) {
			return a.(*v1Entry).Marshal()
		},
		func(b []byte) (any, error) {
			record := new(v1Entry)
			err := record.Unmarshal(b)
			return record, err
		},
	)
}

func DecodeLogEntry(b []byte) (e LogEntry, err error) {
	header := objectio.DecodeIOEntryHeader(b[:objectio.IOEntryHeaderSize])
	switch header.Version {
	case IOET_WALRecord_V1:
		var (
			v1     v1Entry
			footer LogEntryFooter
			offset = 0
		)
		if err = v1.Unmarshal(b[objectio.IOEntryHeaderSize:]); err != nil {
			return
		}

		if v1.cmdType == Cmd_Normal {
			e = NewLogEntry()
			for i := 0; i < len(v1.addr); i++ {
				tmp := entry.NewEmptyEntry()
				var n int64
				if n, err = tmp.UnmarshalBinary(v1.payload[offset:]); err != nil {
					return
				}
				off, length := e.AppendEntry(v1.payload[offset : offset+int(n)])
				footer.AppendEntry(off, length)
				offset += int(n)
			}
			e.SetFooter(footer)
		} else if v1.cmdType == Cmd_SkipDSN {
			e = SkipMapToLogEntry(v1.cmd.skipMap)
		}

		e.SetHeader(header.Type, header.Version, uint16(v1.cmdType))
		e.SetSafeDSN(v1.appended)
		minDSN := v1.GetMinDSN()
		e.SetStartDSN(minDSN)

		return
	case IOET_WALRecord_V2:
		e = LogEntry(b)
		return
	default:
		panic(fmt.Sprintf("unsupported version %d", header.Version))
	}
}
