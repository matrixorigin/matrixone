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
	"encoding/base64"
	"encoding/binary"
	"hash/crc32"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

func TestMarkerRoundTripAndScopeBinding(t *testing.T) {
	binding := Binding{Epoch: [16]byte{1}, Observer: Observer{AccountID: 23, Incarnation: 42}}
	marker := Marker{
		To:      timestamp.Timestamp{PhysicalTime: 2000, LogicalTime: 11},
		Sources: map[uint32][16]byte{29: {0x55}, 2: {0xaa}},
	}
	encoded, err := EncodeMarker(binding, marker)
	require.NoError(t, err)
	again, err := EncodeMarker(binding, marker)
	require.NoError(t, err)
	require.Equal(t, encoded, again)
	decoded, err := DecodeMarker(encoded, binding, marker.To)
	require.NoError(t, err)
	require.Equal(t, marker, decoded)

	for name, wrongBinding := range map[string]Binding{
		"epoch":       {Epoch: [16]byte{2}, Observer: binding.Observer},
		"account":     {Epoch: binding.Epoch, Observer: Observer{AccountID: 24, Incarnation: 42}},
		"incarnation": {Epoch: binding.Epoch, Observer: Observer{AccountID: 23, Incarnation: 43}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := DecodeMarker(encoded, wrongBinding, marker.To)
			require.Error(t, err)
		})
	}
	_, err = DecodeMarker(encoded, binding, marker.To.Prev())
	require.ErrorContains(t, err, "watermark")
}

func TestMarkerRejectsInvalidWireData(t *testing.T) {
	binding := Binding{Epoch: [16]byte{1}, Observer: Observer{AccountID: 22, Incarnation: 42}}
	marker := Marker{
		To:      timestamp.Timestamp{PhysicalTime: 1000, LogicalTime: 9},
		Sources: map[uint32][16]byte{3: {1}, 5: {2}},
	}
	encoded, err := EncodeMarker(binding, marker)
	require.NoError(t, err)
	raw, err := base64.StdEncoding.DecodeString(encoded)
	require.NoError(t, err)

	corrupt := slices.Clone(raw)
	corrupt[markerHeaderSize+5] ^= 1
	encodeModified := func(change func(data []byte)) string {
		payload := slices.Clone(raw[:len(raw)-markerCRCSize])
		change(payload)
		payload = binary.BigEndian.AppendUint32(payload, crc32.Checksum(payload, markerCRC))
		return base64.StdEncoding.EncodeToString(payload)
	}
	for name, candidate := range map[string]string{
		"empty":       "",
		"line breaks": encoded[:4] + "\n" + encoded[4:],
		"oversize":    strings.Repeat("A", MaxMarkerBytes+1),
		"truncated":   encoded[:len(encoded)-4],
		"corrupt":     base64.StdEncoding.EncodeToString(corrupt),
		"version":     encodeModified(func(data []byte) { data[len(markerVersion)-1]++ }),
		"count":       encodeModified(func(data []byte) { data[markerHeaderSize-1]++ }),
		"duplicate": encodeModified(func(data []byte) {
			binary.BigEndian.PutUint32(data[markerHeaderSize+markerSourceSize:], 3)
		}),
		"unordered": encodeModified(func(data []byte) {
			binary.BigEndian.PutUint32(data[markerHeaderSize+markerSourceSize:], 1)
		}),
	} {
		t.Run(name, func(t *testing.T) {
			decoded, err := DecodeMarker(candidate, binding, marker.To)
			require.Error(t, err)
			require.Empty(t, decoded.Sources)
		})
	}
}

func TestMarkerLargeSourceBounds(t *testing.T) {
	binding := Binding{Epoch: [16]byte{1}, Observer: Observer{AccountID: 22, Incarnation: 42}}
	marker := Marker{
		To:      timestamp.Timestamp{PhysicalTime: 1000, LogicalTime: 9},
		Sources: make(map[uint32][16]byte, MaxSources),
	}
	for i := uint32(1); i <= 5_000; i++ {
		marker.Sources[i] = [16]byte{byte(i)}
	}
	encoded, err := EncodeMarker(binding, marker)
	require.NoError(t, err)
	require.Greater(t, len(encoded), 65535)
	decoded, err := DecodeMarker(encoded, binding, marker.To)
	require.NoError(t, err)
	require.Equal(t, marker, decoded)

	for i := uint32(5_001); i <= MaxSources; i++ {
		marker.Sources[i] = [16]byte{byte(i)}
	}
	encoded, err = EncodeMarker(binding, marker)
	require.NoError(t, err)
	require.Len(t, encoded, MaxMarkerBytes)
	decoded, err = DecodeMarker(encoded, binding, marker.To)
	require.NoError(t, err)
	require.Len(t, decoded.Sources, MaxSources)
	require.Equal(t, marker.Sources[MaxSources], decoded.Sources[MaxSources])
	marker.Sources[MaxSources+1] = [16]byte{}
	_, err = EncodeMarker(binding, marker)
	require.Error(t, err)
}

func TestProjectGrantsTracksPerSourceAndPhysicalIdentity(t *testing.T) {
	a := Grant{AccountID: 2, DatabaseName: "db", TableName: "active", GrantID: [16]byte{1}, DatabaseID: 11, TableID: 31}
	missing := Grant{AccountID: 2, DatabaseName: "db", TableName: "missing", GrantID: [16]byte{2}, DatabaseID: 11}
	retired := Grant{AccountID: 4, DatabaseName: "db", TableName: "retired", GrantID: [16]byte{3}, DatabaseID: 12, TableID: 34}

	prior, baseline, incremental, err := ProjectGrants([]Grant{missing, retired, a}, nil)
	require.NoError(t, err)
	require.Equal(t, []Grant{a, missing, retired}, baseline)
	require.Empty(t, incremental)

	next, baseline, incremental, err := ProjectGrants([]Grant{missing, a}, prior)
	require.NoError(t, err)
	require.Empty(t, baseline)
	require.Equal(t, []Grant{a}, incremental)
	require.Len(t, next, 1)
	require.Equal(t, prior[2], next[2])

	for name, change := range map[string]func(*Grant){
		"regrant":    func(grant *Grant) { grant.GrantID[0]++ },
		"rebuild":    func(grant *Grant) { grant.TableID++ },
		"drop":       func(grant *Grant) { grant.TableID = 0 },
		"db rebuild": func(grant *Grant) { grant.DatabaseID++ },
		"rename":     func(grant *Grant) { grant.TableName = "renamed" },
	} {
		t.Run(name, func(t *testing.T) {
			replacement := a
			change(&replacement)
			digests, baseline, incremental, err := ProjectGrants([]Grant{retired, replacement, missing}, prior)
			require.NoError(t, err)
			require.NotEqual(t, prior[2], digests[2])
			require.Equal(t, prior[4], digests[4])
			require.ElementsMatch(t, []Grant{replacement, missing}, baseline)
			require.Equal(t, []Grant{retired}, incremental)
		})
	}
	_, _, _, err = ProjectGrants([]Grant{a, a}, prior)
	require.ErrorContains(t, err, "duplicate")
	_, _, _, err = ProjectGrants(make([]Grant, MaxTables+1), prior)
	require.ErrorContains(t, err, "limit")
}
