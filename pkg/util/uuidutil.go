// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package util

import (
	"bytes"
	"context"
	"encoding/binary"
	"hash/fnv"
	"net"

	"github.com/google/uuid"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// getDefaultHardwareAddr returns hardware address(like mac addr), like the first valid val.
var getDefaultHardwareAddr = func(ctx context.Context) (net.HardwareAddr, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return []byte{}, err
	}
	for _, iface := range ifaces {
		if len(iface.HardwareAddr) >= 6 {
			return iface.HardwareAddr, nil
		}
	}
	return []byte{}, moerr.NewInternalError(ctx, "uuid: no Hardware address found")
}

// docker mac addr range: [02:42:ac:11:00:00, 02:42:ac:11:ff:ff]
var dockerMacPrefix = []byte{0x02, 0x42, 0xac}

// SetUUIDNodeID set all uuid generator's node_id
func SetUUIDNodeID(ctx context.Context, nodeUuid []byte) error {
	var err error
	var hwAddr []byte
	// situation 1: set by mac addr
	hwAddr, err = getDefaultHardwareAddr(ctx)
	if err == nil && !bytes.Equal(hwAddr[:len(dockerMacPrefix)], dockerMacPrefix) && uuid.SetNodeID(hwAddr) {
		return nil
	}
	// case 2: set by nodeUuid prefix
	if len(nodeUuid) > 6 && uuid.SetNodeID(nodeUuid) {
		return nil
	}
	// case 3: set by md5sum{hwAddr + nodeUuid, randomUint64}
	var num [8]byte
	binary.BigEndian.PutUint64(num[:], Fastrand64())
	hasher := fnv.New128()
	if _, err = hasher.Write(hwAddr); err != nil {
		return err
	} else if _, err := hasher.Write(nodeUuid); err != nil {
		return err
	} else if _, err := hasher.Write(num[:]); err != nil {
		return err
	}
	var hash = make([]byte, 0, 16)
	hash = hasher.Sum(hash[:])
	if !uuid.SetNodeID(hash[:]) {
		return moerr.NewInternalError(ctx, "uuid: nodeID too short")
	}
	return nil
}
