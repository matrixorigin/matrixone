// Copyright 2021 - 2024 Matrix Origin
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

package datasync

import (
	"bytes"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/logservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/driver/logservicedriver"
	entry2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/entry"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/store"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

const (
	headerSize        = 4
	replicaIDSize     = 8
	payloadHeaderSize = 4
	dataHeaderSize    = headerSize + replicaIDSize + payloadHeaderSize
	entryHeaderSize   = 4
)

// getLocations checks if the cmd is file-action in storage.
// If it is, return the location of the file.
func getLocations(rec logservice.LogRecord, tag string) []string {
	if rec.Type != logservice.UserRecord {
		return nil
	}
	data := rec.Data
	if len(data) < dataHeaderSize {
		logutil.Errorf("invalid data size %d", len(data))
		return nil
	}
	buffer := bytes.NewBuffer(data[dataHeaderSize:])
	m := &logservicedriver.V1Meta{}
	_, err := m.ReadFrom(buffer)
	if err != nil {
		logutil.Errorf("failed to read data from buffer: %v", err)
		return nil
	}
	if m.GetType() != logservicedriver.Cmd_Normal {
		return nil
	}
	var locations []string
	for range m.GetAddr() {
		e := entry.NewEmptyEntry()
		_, err := e.ReadFrom(buffer)
		if err != nil {
			logutil.Errorf("failed to read data from buffer: %v", err)
			return nil
		}
		ei := e.Entry.GetInfo().(*entry2.Info)
		payload := e.Entry.GetPayload()
		if ei.Group == wal.GroupPrepare {
			locations = append(locations, parseCommonFiles(payload, tag)...)
		} else if ei.Group == store.GroupFiles {
			locations = append(locations, parseMetaFiles(payload, tag)...)
		}
	}
	return locations
}

// parseCommonFiles parses the common files which are in the root directory.
func parseCommonFiles(payload []byte, tag string) []string {
	if len(payload) < entryHeaderSize {
		return nil
	}
	head := objectio.DecodeIOEntryHeader(payload[:entryHeaderSize])
	codec := objectio.GetIOEntryCodec(*head)
	ent, err := codec.Decode(payload[entryHeaderSize:])
	if err != nil {
		logutil.Errorf("failed to decode entry: %v", err)
		return nil
	}
	txnCmd, ok := ent.(*txnbase.TxnCmd)
	if !ok {
		return nil
	}
	if txnCmd.ComposedCmd == nil {
		return nil
	}
	var locations []string
	for _, cmd := range txnCmd.ComposedCmd.Cmds {
		// there will not be this type in 1.3 version, so ignore this one.
		// if catalog.IOET_WALTxnCommand_Block == cmd.GetType() {
		// 	return true
		// }

		if catalog.IOET_WALTxnCommand_Object != cmd.GetType() {
			continue
		}
		entryCmd, ok := cmd.(*catalog.EntryCommand[*catalog.ObjectMVCCNode, *catalog.ObjectNode])
		if !ok {
			continue
		}
		mvccNode := entryCmd.GetMVCCNode()
		if mvccNode != nil && mvccNode.BaseNode != nil && !mvccNode.BaseNode.IsEmpty() {
			locations = append(locations, mvccNode.BaseNode.ObjectLocation().String())
		}
	}
	// TODO(volgariver6): remove the following log.
	if len(tag) > 0 && len(locations) > 0 {
		logutil.Infof("parsed common files: %v, txn: [%X,%s], tag: %s",
			locations,
			txnCmd.GetID(),
			txnCmd.GetPrepareTS().ToString(),
			tag,
		)
	}
	return locations
}

// parseMetaFiles parses the meta files which are in ckp/ or gc/ directory.
func parseMetaFiles(payload []byte, tag string) []string {
	vec := vector.NewVec(types.Type{})
	if err := vec.UnmarshalBinary(payload); err != nil {
		logutil.Errorf("failed to unmarshal checkpoint file: %v", err)
		return nil
	}
	var ignore bool
	var locations []string
	for i := 0; i < vec.Length(); i++ {
		file := vec.GetStringAt(i)
		if strings.Contains(file, "gc/") {
			ignore = true
			break
		}
		locations = append(locations, file)
	}
	if ignore {
		return nil
	}
	if len(tag) > 0 && len(locations) > 0 {
		logutil.Infof("parsed meta files: %v, tag: %s", locations, tag)
	}
	return locations
}
