// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collationkey

import "bytes"

const (
	// LegacyVersion means that no versioned key contract is recorded.  It is
	// intentionally not a claim that the relation is collation-aware.
	LegacyVersion uint32 = 0
	// BytewiseVersion identifies an explicit bytewise physical-key format.
	BytewiseVersion uint32 = 1
	// CollationAwareVersion identifies the framed key format owned by this
	// package.  It is not enabled for storage until the activation fence is
	// satisfied by every writer and reader.
	CollationAwareVersion uint32 = uint32(CodecVersion)
)

// RelationMetadata is the typed identity carried by a physical primary or
// UNIQUE relation.  A zero value is legacy metadata; callers must still
// distinguish absence from a relation explicitly requesting v2 before
// admitting a write.
type RelationMetadata struct {
	Version            uint32
	RegistryVersion    uint32
	RegistryDigest     []byte
	MaxEncodedKeyBytes uint32
}

// Capability is the node-local portion of the v2 admission contract.
// Versions are represented as bits in ReadableVersions/WritableVersions, so a
// node can continue serving legacy relations while it is prepared for v2.
type Capability struct {
	ReadableVersions   uint32
	WritableVersions   uint32
	RegistryVersion    uint32
	RegistryDigest     []byte
	MaxEncodedKeyBytes uint32
}

func NewCollationAwareMetadata() RelationMetadata {
	return RelationMetadata{
		Version:            CollationAwareVersion,
		RegistryVersion:    uint32(RegistryVersion),
		RegistryDigest:     RegistryDigest(),
		MaxEncodedKeyBytes: MaxKeyBytes,
	}
}

func (m RelationMetadata) IsLegacy() bool { return m.Version == LegacyVersion }
func (m RelationMetadata) IsV2() bool     { return m.Version == CollationAwareVersion }

func (m RelationMetadata) Validate() error {
	switch m.Version {
	case LegacyVersion, BytewiseVersion:
		if len(m.RegistryDigest) != 0 || m.RegistryVersion != 0 || m.MaxEncodedKeyBytes != 0 {
			return wrapCodecError(ErrMalformedKey, "legacy relation carries v2 metadata")
		}
		return nil
	case CollationAwareVersion:
		if m.RegistryVersion != uint32(RegistryVersion) {
			return wrapCodecError(ErrMalformedKey, "registry version %d", m.RegistryVersion)
		}
		if !bytes.Equal(m.RegistryDigest, RegistryDigest()) {
			return wrapCodecError(ErrMalformedKey, "registry digest mismatch")
		}
		if m.MaxEncodedKeyBytes != MaxKeyBytes {
			return wrapCodecError(ErrMalformedKey, "maximum key bytes %d", m.MaxEncodedKeyBytes)
		}
		return nil
	default:
		return wrapCodecError(ErrMalformedKey, "unknown relation key version %d", m.Version)
	}
}

func (c Capability) Validate() error {
	if c.RegistryVersion != uint32(RegistryVersion) {
		return wrapCodecError(ErrMalformedKey, "capability registry version %d", c.RegistryVersion)
	}
	if !bytes.Equal(c.RegistryDigest, RegistryDigest()) {
		return wrapCodecError(ErrMalformedKey, "capability registry digest mismatch")
	}
	if c.MaxEncodedKeyBytes != MaxKeyBytes {
		return wrapCodecError(ErrMalformedKey, "capability maximum key bytes %d", c.MaxEncodedKeyBytes)
	}
	return nil
}

func (c Capability) Supports(m RelationMetadata, write bool) bool {
	if m.Validate() != nil || c.Validate() != nil || m.Version >= 32 {
		return false
	}
	versions := c.ReadableVersions
	if write {
		versions = c.WritableVersions
	}
	return versions&(uint32(1)<<m.Version) != 0
}
