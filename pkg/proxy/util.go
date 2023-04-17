// Copyright 2021 - 2023 Matrix Origin
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

package proxy

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/frontend"
)

// makeOKPacket returns an OK packet
func makeOKPacket() []byte {
	l := 1
	data := make([]byte, l+4)
	data[4] = 0
	data[0] = byte(l)
	data[1] = byte(l >> 8)
	data[2] = byte(l >> 16)
	data[3] = 0
	return data
}

// isOKPacket returns true if []byte is a MySQL OK packet.
func isOKPacket(p []byte) bool {
	if len(p) > 4 && p[4] == 0 {
		return true
	}
	return false
}

// isOKPacket returns true if []byte is a MySQL EOF packet.
func isEOFPacket(p []byte) bool {
	if len(p) > 4 && p[0] == 0xFE {
		return true
	}
	return false
}

// isErrPacket returns true if []byte is a MySQL Err packet.
func isErrPacket(p []byte) bool {
	if len(p) > 4 && p[4] == 0xFF {
		return true
	}
	return false
}

// packetToBytes convert Packet to bytes.
func packetToBytes(p *frontend.Packet) []byte {
	if p == nil || len(p.Payload) == 0 {
		return nil
	}
	res := make([]byte, 4, 4+len(p.Payload))
	length := len(p.Payload)
	res[0] = byte(length)
	res[1] = byte(length >> 8)
	res[2] = byte(length >> 16)
	res[3] = byte(p.SequenceID)
	return append(res, p.Payload...)
}

// bytesToPacket convert bytes to Packet.
func bytesToPacket(bs []byte) *frontend.Packet {
	if len(bs) < 4 {
		return nil
	}
	p := &frontend.Packet{
		Length:     int32(bs[0]) | int32(bs[1])<<8 | int32(bs[2])<<8,
		SequenceID: int8(bs[3]),
		Payload:    bs[4:],
	}
	return p
}

// getStatement gets a statement from message bytes which is MySQL protocol.
func getStatement(msg []byte) string {
	return string(msg[5:])
}

// pickTunnels pick N tunnels from the given tunnels. Simply, just
// pick the first N tunnels.
func pickTunnels(tuns tunnelSet, n int) []*tunnel {
	if len(tuns) == 0 || n <= 0 {
		return nil
	}
	size := n
	if len(tuns) < n {
		size = len(tuns)
	}
	ret := make([]*tunnel, 0, size)
	i := 1
	for t := range tuns {
		ret = append(ret, t)
		i++
		if i > size {
			break
		}
	}
	return ret
}

// isStmtBegin returns true iff it is begin statement.
func isStmtBegin(c []byte) bool {
	if len(c) != 5 {
		return false
	}
	return (c[0] == 'b' || c[0] == 'B') &&
		(c[1] == 'e' || c[1] == 'E') &&
		(c[2] == 'g' || c[2] == 'G') &&
		(c[3] == 'i' || c[3] == 'I') &&
		(c[4] == 'n' || c[4] == 'N')
}

// isStmtCommit returns true iff it is commit statement.
func isStmtCommit(c []byte) bool {
	if len(c) != 6 {
		return false
	}
	return (c[0] == 'c' || c[0] == 'C') &&
		(c[1] == 'o' || c[1] == 'O') &&
		(c[2] == 'm' || c[2] == 'M') &&
		(c[3] == 'm' || c[3] == 'M') &&
		(c[4] == 'i' || c[4] == 'I') &&
		(c[5] == 't' || c[5] == 'T')
}

// isStmtRollback returns true iff it is rollback statement.
func isStmtRollback(c []byte) bool {
	if len(c) != 8 {
		return false
	}
	return (c[0] == 'r' || c[0] == 'R') &&
		(c[1] == 'o' || c[1] == 'O') &&
		(c[2] == 'l' || c[2] == 'L') &&
		(c[3] == 'l' || c[3] == 'L') &&
		(c[4] == 'b' || c[4] == 'B') &&
		(c[5] == 'a' || c[5] == 'A') &&
		(c[6] == 'c' || c[6] == 'C') &&
		(c[7] == 'k' || c[7] == 'K')
}

// sortMap sorts a complex map instance.
func sortMap(target map[string]any) map[string]any {
	sorted := sortSimpleMap(target)
	res := make(map[string]any)
	for k, v := range sorted {
		if tv, s := v.(map[string]any); s {
			res[k] = sortMap(tv)
		} else if tv, s := v.([]any); s {
			res[k] = sortSlice(tv)
		} else {
			res[k] = v
		}
	}
	return res
}

// sortSlice sorts a slice instance.
func sortSlice(target []any) []any {
	hashArr := make(map[string]any)
	for _, i := range target {
		var tmpV any
		var ha string
		if ttv, ts := i.(map[string]any); ts {
			tmpV = sortMap(ttv)
			ha = rawHash(tmpV)
		} else if ttv, ts := i.([]any); ts {
			tmpV = sortSlice(ttv)
			ha = rawHash(tmpV)
		} else {
			tmpV = i
			ha = tmpV.(string)
		}
		hashArr[ha] = tmpV
	}

	sor := sortSimpleMap(hashArr)
	sortKeys := getSortKeys(sor)
	r := make([]any, 0, len(sortKeys))
	for _, v := range sortKeys {
		r = append(r, sor[v])
	}
	return r
}

// sortSimpleMap sort simple map by keys.
func sortSimpleMap(target map[string]any) map[string]any {
	keys := getSortKeys(target)
	res := make(map[string]any, len(keys))
	for _, k := range keys {
		res[k] = target[k]
	}
	return res
}

// getSortKeys returns sorted keys in the map.
func getSortKeys(target map[string]any) []string {
	keys := make([]string, 0, len(target))
	for k := range target {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// rawHash returns a string value as the hash result.
func rawHash(t any) string {
	sortBytes, err := json.Marshal(t)
	if err != nil {
		return ""
	}
	hash := md5.Sum(sortBytes)
	return hex.EncodeToString(hash[:])
}
