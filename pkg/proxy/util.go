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
	"net"
	"sort"

	"github.com/matrixorigin/matrixone/pkg/clusterservice"
	"github.com/matrixorigin/matrixone/pkg/frontend"
	"github.com/matrixorigin/matrixone/pkg/pb/metadata"
)

// makeOKPacket returns an OK packet
func makeOKPacket(l int) []byte {
	data := make([]byte, l+4)
	data[4] = 0
	data[0] = byte(l)
	data[1] = byte(l >> 8)
	data[2] = byte(l >> 16)
	data[3] = 1
	return data
}

// makeErrPacket returns an ERROR packet
func makeErrPacket(l int) []byte {
	data := make([]byte, l+4)
	data[4] = 0xFF
	data[0] = byte(l)
	data[1] = byte(l >> 8)
	data[2] = byte(l >> 16)
	data[3] = 1
	return data
}

func makeQuitPacket() []byte {
	data := make([]byte, 5)
	data[4] = byte(cmdQuit)
	data[0] = 1
	data[3] = 1
	return data
}

func isCmdQuery(p []byte) bool {
	if len(p) > 4 && p[4] == byte(cmdQuery) {
		return true
	}
	return false
}

func isCmdQuit(p []byte) bool {
	if len(p) > 4 && p[4] == byte(cmdQuit) {
		return true
	}
	return false
}

func isCmdInitDB(p []byte) bool {
	if len(p) > 4 && p[4] == byte(cmdInitDB) {
		return true
	}
	return false
}

func isCmdStmtPrepare(p []byte) bool {
	if len(p) > 4 && p[4] == byte(cmdStmtPrepare) {
		return true
	}
	return false
}

func isCmdStmtClose(p []byte) bool {
	if len(p) > 4 && p[4] == byte(cmdStmtClose) {
		return true
	}
	return false
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
	if len(p) > 4 && p[4] == 0xFE {
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

// isLoadDataLocalInfileRespPacket returns true if []byte is a packet
// of load data local infile response.
func isLoadDataLocalInfileRespPacket(p []byte) bool {
	if len(p) > 4 && p[4] == 0xFB {
		return true
	}
	return false
}

// isEmptyPacket returns true if []byte is an empty packet.
func isEmptyPacket(p []byte) bool {
	return len(p) == 0
}

// isDeallocatePacket returns true if []byte is a MySQL
func isDeallocatePacket(p []byte) bool {
	if len(p) > 4 && p[4] == 0x19 {
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
		Length:     int32(bs[0]) | int32(bs[1])<<8 | int32(bs[2])<<16,
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
		// if the tunnel is in transfer intent state, we need to put it
		// into the queue to speed up its transfer, and it does not count
		// in the 'size'.
		if t.transferIntent.Load() {
			ret = append(ret, t)
			continue
		}
		ret = append(ret, t)
		i++
		if i > size {
			break
		}
	}
	return ret
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

// containIP returns if the list of net.IPNet contains the IP address.
func containIP(ipNetList []*net.IPNet, ip net.IP) bool {
	for _, ipNet := range ipNetList {
		if ipNet.Contains(ip) {
			return true
		}
	}
	return false
}

// getQueryAddress gets the query server address from mo cluster service.
// the second parameter is the SQL address.
func getQueryAddress(mc clusterservice.MOCluster, sqlAddr string) string {
	var queryAddr string
	mc.GetCNService(clusterservice.NewSelectAll(), func(service metadata.CNService) bool {
		if service.SQLAddress == sqlAddr {
			queryAddr = service.QueryAddress
			return false
		}
		return true
	})
	return queryAddr
}
