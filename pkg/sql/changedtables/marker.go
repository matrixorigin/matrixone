// Copyright 2026 Matrix Origin
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

package changedtables

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"maps"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

const (
	MaxTables  = 100_000
	MaxSources = 100_000

	markerVersion    = "MOCT\x01"
	markerHeaderSize = len(markerVersion) + 16 + 4 + 8 + 8 + 4 + 4
	markerSourceSize = 4 + 16
	markerCRCSize    = 4
	maxMarkerPayload = markerHeaderSize + MaxSources*markerSourceSize + markerCRCSize
	MaxMarkerBytes   = (maxMarkerPayload + 2) / 3 * 4
)

var markerCRC = crc32.MakeTable(crc32.Castagnoli)

// Observer identifies an account instance using its catalog creation time.
type Observer struct {
	AccountID   uint32
	Incarnation uint64
}

// Binding describes the authorities that must still match when a marker is replayed.
// The recovery epoch is provisioned as a new random UUID for each restored or cloned instance.
type Binding struct {
	Epoch    [16]byte
	Observer Observer
}

type Marker struct {
	To      timestamp.Timestamp
	Sources map[uint32][16]byte
}

// EncodeMarker encodes a canonical, unpadded-in-the-payload binary record with
// a CRC32C for accidental corruption. The outer base64 uses standard padding.
func EncodeMarker(binding Binding, current Marker) (string, error) {
	if binding.Epoch == ([16]byte{}) || current.To.PhysicalTime <= 0 {
		return "", errors.New("invalid marker binding or watermark")
	}
	if len(current.Sources) > MaxSources {
		return "", errors.New("marker exceeds source limit")
	}

	data := make([]byte, 0, markerHeaderSize+len(current.Sources)*markerSourceSize+markerCRCSize)
	data = append(data, markerVersion...)
	data = append(data, binding.Epoch[:]...)
	data = binary.BigEndian.AppendUint32(data, binding.Observer.AccountID)
	data = binary.BigEndian.AppendUint64(data, binding.Observer.Incarnation)
	data = binary.BigEndian.AppendUint64(data, uint64(current.To.PhysicalTime))
	data = binary.BigEndian.AppendUint32(data, current.To.LogicalTime)
	data = binary.BigEndian.AppendUint32(data, uint32(len(current.Sources)))
	for _, account := range slices.Sorted(maps.Keys(current.Sources)) {
		data = binary.BigEndian.AppendUint32(data, account)
		scope := current.Sources[account]
		data = append(data, scope[:]...)
	}
	data = binary.BigEndian.AppendUint32(data, crc32.Checksum(data, markerCRC))
	return base64.StdEncoding.EncodeToString(data), nil
}

// DecodeMarker validates the marker before using its source authorization digests.
// Callers distinguish SQL NULL (bootstrap) from a present marker before decoding.
func DecodeMarker(encoded string, binding Binding, to timestamp.Timestamp) (Marker, error) {
	if binding.Epoch == ([16]byte{}) || len(encoded) > MaxMarkerBytes || strings.ContainsAny(encoded, "\r\n") {
		return Marker{}, errors.New("invalid marker binding or encoded length")
	}
	data, err := base64.StdEncoding.Strict().DecodeString(encoded)
	if err != nil || len(data) < markerHeaderSize+markerCRCSize || len(data) > maxMarkerPayload {
		return Marker{}, errors.New("invalid marker encoding")
	}
	if string(data[:len(markerVersion)]) != markerVersion {
		return Marker{}, errors.New("unsupported marker version")
	}
	data, checksum := data[:len(data)-markerCRCSize], data[len(data)-markerCRCSize:]
	if crc32.Checksum(data, markerCRC) != binary.BigEndian.Uint32(checksum) {
		return Marker{}, errors.New("invalid marker checksum")
	}
	data = data[len(markerVersion):]
	if !bytes.Equal(data[:16], binding.Epoch[:]) ||
		binary.BigEndian.Uint32(data[16:20]) != binding.Observer.AccountID ||
		binary.BigEndian.Uint64(data[20:28]) != binding.Observer.Incarnation {
		return Marker{}, errors.New("marker belongs to a different recovery epoch or observer")
	}
	cut := timestamp.Timestamp{
		PhysicalTime: int64(binary.BigEndian.Uint64(data[28:36])),
		LogicalTime:  binary.BigEndian.Uint32(data[36:40]),
	}
	if cut.PhysicalTime <= 0 || cut.Greater(to) {
		return Marker{}, errors.New("invalid marker watermark")
	}
	count := binary.BigEndian.Uint32(data[40:44])
	data = data[44:]
	if count > MaxSources || len(data) != int(count)*markerSourceSize {
		return Marker{}, errors.New("invalid marker source count")
	}

	scopes := make(map[uint32][16]byte, count)
	var previous uint32
	for i := uint32(0); i < count; i++ {
		account := binary.BigEndian.Uint32(data[:4])
		if i > 0 && account <= previous {
			return Marker{}, errors.New("marker sources are not sorted and unique")
		}
		scopes[account] = [16]byte(data[4:markerSourceSize])
		data = data[markerSourceSize:]
		previous = account
	}
	return Marker{To: cut, Sources: scopes}, nil
}

// Grant contains canonical names and the catalog identifiers visible at the cut.
// A missing relation has TableID zero even when the database still exists.
type Grant struct {
	AccountID    uint32
	DatabaseName string
	TableName    string
	GrantID      [16]byte
	DatabaseID   uint64
	TableID      uint64
}

// ProjectGrants compares each current source scope with its previously covered
// digest. It sorts grants in place. Missing relations remain in baseline rows
// and in the digest, while only extant incremental relations can reach the TN.
func ProjectGrants(grants []Grant, prior map[uint32][16]byte) (
	digests map[uint32][16]byte, baseline, incremental []Grant, err error,
) {
	if len(grants) > MaxTables {
		return nil, nil, nil, errors.New("grant scope exceeds table limit")
	}
	slices.SortFunc(grants, func(a, b Grant) int {
		if a.AccountID != b.AccountID {
			if a.AccountID < b.AccountID {
				return -1
			}
			return 1
		}
		if order := strings.Compare(a.DatabaseName, b.DatabaseName); order != 0 {
			return order
		}
		return strings.Compare(a.TableName, b.TableName)
	})

	digests = make(map[uint32][16]byte)
	hash := sha256.New()
	var ordinal [4]byte
	var ids [16]byte
	for len(grants) != 0 {
		account := grants[0].AccountID
		groupEnd := 0
		hash.Reset()
		for ; groupEnd < len(grants) && grants[groupEnd].AccountID == account; groupEnd++ {
			grant := grants[groupEnd]
			if grant.DatabaseName == "" || grant.TableName == "" || (grant.TableID != 0 && grant.DatabaseID == 0) {
				return nil, nil, nil, errors.New("invalid granted table identity")
			}
			if groupEnd != 0 {
				last := grants[groupEnd-1]
				if grant.DatabaseName == last.DatabaseName && grant.TableName == last.TableName {
					return nil, nil, nil, errors.New("duplicate granted table name")
				}
			}
			binary.BigEndian.PutUint32(ordinal[:], uint32(len(grant.DatabaseName)))
			hash.Write(ordinal[:])
			hash.Write([]byte(grant.DatabaseName))
			binary.BigEndian.PutUint32(ordinal[:], uint32(len(grant.TableName)))
			hash.Write(ordinal[:])
			hash.Write([]byte(grant.TableName))
			hash.Write(grant.GrantID[:])
			binary.BigEndian.PutUint64(ids[:8], grant.DatabaseID)
			binary.BigEndian.PutUint64(ids[8:], grant.TableID)
			hash.Write(ids[:])
		}
		var sum [sha256.Size]byte
		hash.Sum(sum[:0])
		scope := [16]byte(sum[:16])
		digests[account] = scope
		before, exists := prior[account]
		if !exists || before != scope {
			baseline = append(baseline, grants[:groupEnd]...)
		} else {
			for _, grant := range grants[:groupEnd] {
				if grant.TableID != 0 {
					incremental = append(incremental, grant)
				}
			}
		}
		grants = grants[groupEnd:]
	}
	return digests, baseline, incremental, nil
}
