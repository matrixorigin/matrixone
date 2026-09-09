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

// Package collationkey owns the versioned identity encoding used by future
// collation-aware primary and unique-key relations.  It deliberately has no
// dependency on planner, executor, aggregation, or storage packages: all
// producers and consumers must be able to share this byte contract.
//
// This package is a foundation layer.  It does not enable a storage relation
// by itself; relation metadata and capability gates are required before these
// bytes may be persisted.
package collationkey

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math"
	"math/big"
	"strings"
	"unicode/utf8"

	"github.com/cespare/xxhash/v2"
)

const (
	magic           = "MOKY"
	CodecVersion    = uint8(2)
	RegistryName    = "collationkey/domains-v1"
	RegistryVersion = uint8(1)
	MaxParts        = 16
	MaxKeyBytes     = 64 << 20
	maxParameter    = 1<<16 - 1
	maxPayload      = 1<<32 - 1
)

// TypeFamily identifies the type family whose value is normalized.  A family
// is deliberately narrower than a SQL OID: a new SQL type must be registered
// with an identity policy before it can use this format.
type TypeFamily uint8

const (
	Text            TypeFamily = 1
	Binary          TypeFamily = 2
	SignedInteger   TypeFamily = 3
	UnsignedInteger TypeFamily = 4
	Decimal         TypeFamily = 5
)

// PrefixUnit is the unit used by Domain.Prefix.  Text prefixes count decoded
// Unicode code points; binary prefixes count bytes.
type PrefixUnit uint8

const (
	PrefixCharacters PrefixUnit = 1
	PrefixBytes      PrefixUnit = 2
)

// These values mirror the current MatrixOne text identities without importing
// pkg/container/types.  CharsetLegacy is intentionally not accepted for a v2
// text domain: old zero-valued metadata must retain historical byte semantics.
const (
	CharsetLegacy     uint8 = 0
	CharsetBinary     uint8 = 1
	CharsetUTF8MB4Bin uint8 = 2
	CharsetUTF8       uint8 = 3
)

// Domain describes the fixed comparison domain of one index part.  Prefix is
// applied after SQL type conversion/length enforcement and before collation
// normalization. Width is a fixed byte width for integer/decimal input;
// Scale is the declared decimal scale. Collation is reserved for a future
// explicit catalog collation identifier; zero is the normalized current
// general-ci/_bin identity carried by Charset.
type Domain struct {
	Type      TypeFamily
	Charset   uint8
	Collation uint16
	Prefix    uint32
	Unit      PrefixUnit
	Width     uint16
	Scale     int16
}

// Part is one user value in an encoded key.  For Null parts Value is ignored.
// Non-null numeric values use the input conventions documented by
// EncodePart: fixed-width big-endian bytes for integers and an ASCII decimal
// literal for Decimal.
type Part struct {
	Domain Domain
	Value  []byte
	Null   bool
}

var (
	ErrUnsupportedDomain error = codecSentinel("collationkey: unsupported domain")
	ErrInvalidValue      error = codecSentinel("collationkey: invalid value")
	ErrMalformedKey      error = codecSentinel("collationkey: malformed encoded key")
)

// codecSentinel keeps the exported error identities comparable without
// pulling a general error-construction policy into this foundation package.
type codecSentinel string

func (e codecSentinel) Error() string { return string(e) }

type codecWrappedError struct {
	cause   error
	message string
}

func (e codecWrappedError) Error() string { return e.message }
func (e codecWrappedError) Unwrap() error { return e.cause }

func wrapCodecError(cause error, format string, args ...any) error {
	return codecWrappedError{
		cause:   cause,
		message: cause.Error() + ": " + fmt.Sprintf(format, args...),
	}
}

type familySpec struct {
	id       uint16
	typ      TypeFamily
	charset  uint8
	padSpace bool
	weight   uint8
	params   []byte
}

// The registry is immutable by construction.  IDs identify normalization
// families; per-index prefix/width/scale values live in the canonical part
// descriptor and therefore do not require inventing an ID for every index.
var familySpecs = [...]familySpec{
	{id: 0x0001, typ: Text, charset: CharsetUTF8, padSpace: true, weight: 1, params: []byte{1, 4, 1}},
	{id: 0x0002, typ: Text, charset: CharsetUTF8MB4Bin, padSpace: true, weight: 0, params: []byte{1, 4, 1}},
	{id: 0x0003, typ: Binary, charset: CharsetBinary, padSpace: false, weight: 0, params: []byte{1, 4, 2}},
	{id: 0x0101, typ: SignedInteger, charset: CharsetLegacy, params: []byte{1, 2}},
	{id: 0x0102, typ: UnsignedInteger, charset: CharsetLegacy, params: []byte{1, 2}},
	{id: 0x0103, typ: Decimal, charset: CharsetLegacy, params: []byte{1, 2, 2}},
}

var registryDigest = buildRegistryDigest()

// RegistryDigest returns the immutable registry digest used by metadata and
// capability records.  The returned slice is a copy.
func RegistryDigest() []byte {
	return append([]byte(nil), registryDigest[:]...)
}

func buildRegistryDigest() [sha256.Size]byte {
	var b bytes.Buffer
	b.WriteString("MOKD")
	b.WriteByte(RegistryVersion)
	for _, spec := range familySpecs {
		var id [2]byte
		binary.BigEndian.PutUint16(id[:], spec.id)
		b.Write(id[:])
		entry := registryEntry(spec)
		if len(entry) > math.MaxUint16 {
			panic("collationkey: registry entry is too large")
		}
		var n [2]byte
		binary.BigEndian.PutUint16(n[:], uint16(len(entry)))
		b.Write(n[:])
		b.Write(entry)
	}
	// The general-ci table is executable registry data, not merely a
	// documentation detail. Include a canonical serialization of every
	// explicitly populated plane (and the fallback policy) so two binaries
	// cannot advertise the same registry digest while normalizing different
	// Unicode code points. The plane index and length delimit each table and
	// nil planes are represented explicitly.
	b.WriteString("general-ci-weight-table-v1")
	for plane, weights := range planeTable {
		var index [2]byte
		binary.BigEndian.PutUint16(index[:], uint16(plane))
		b.Write(index[:])
		if weights == nil {
			b.WriteByte(0)
			continue
		}
		b.WriteByte(1)
		var length [4]byte
		binary.BigEndian.PutUint32(length[:], uint32(len(weights)))
		b.Write(length[:])
		var weightBytes [2]byte
		for _, weight := range weights {
			binary.BigEndian.PutUint16(weightBytes[:], weight)
			b.Write(weightBytes[:])
		}
	}
	// Values outside the table deliberately use their code point, while
	// supplementary planes use the frozen replacement weight. Keep both
	// policies in the digest even though they are not table entries.
	b.WriteString("fallback-rune-v1")
	b.WriteString("supplementary-fffd-v1")
	return sha256.Sum256(b.Bytes())
}

// registryEntry is a stable binary description, not a Go struct encoding.
// The schema byte, type/charset/padding/weight fields, and parameter schema
// are all part of the digest.  Legal parameter ranges are explicit so a node
// cannot accept a descriptor with a different interpretation.
func registryEntry(spec familySpec) []byte {
	var b bytes.Buffer
	b.WriteByte(1) // entry schema
	b.WriteByte(byte(spec.typ))
	b.WriteByte(spec.charset)
	if spec.padSpace {
		b.WriteByte(1)
	} else {
		b.WriteByte(0)
	}
	b.WriteByte(spec.weight)
	b.WriteByte(byte(len(spec.params)))
	b.Write(spec.params)
	// Current parameter ranges: prefix is uint32; numeric width is a positive
	// byte width and decimal scale is an int16.  Encoding this explicitly keeps
	// validation part of the versioned registry contract.
	b.Write([]byte{0, 0, 0, 1, 0, 0, 0, 32})
	b.Write([]byte{0, 1, 0, 32})
	b.Write([]byte{0x80, 0, 0x7f, 0xff})
	return b.Bytes()
}

func domainSpec(domain Domain) (familySpec, []byte, error) {
	if domain.Width == 0 &&
		(domain.Type == SignedInteger || domain.Type == UnsignedInteger || domain.Type == Decimal) {
		return familySpec{}, nil, ErrUnsupportedDomain
	}
	if domain.Collation != 0 {
		return familySpec{}, nil, wrapCodecError(ErrUnsupportedDomain, "explicit collation %d is not registered", domain.Collation)
	}
	var spec familySpec
	switch domain.Type {
	case Text:
		if domain.Unit != PrefixCharacters {
			return familySpec{}, nil, wrapCodecError(ErrUnsupportedDomain, "text prefix unit %d", domain.Unit)
		}
		switch domain.Charset {
		case CharsetUTF8:
			spec = familySpecs[0]
		case CharsetUTF8MB4Bin:
			spec = familySpecs[1]
		default:
			return familySpec{}, nil, wrapCodecError(ErrUnsupportedDomain, "text charset %d", domain.Charset)
		}
		return spec, textParams(domain), nil
	case Binary:
		if domain.Charset != CharsetBinary && domain.Charset != CharsetLegacy {
			return familySpec{}, nil, wrapCodecError(ErrUnsupportedDomain, "binary charset %d", domain.Charset)
		}
		if domain.Unit != PrefixBytes {
			return familySpec{}, nil, wrapCodecError(ErrUnsupportedDomain, "binary prefix unit %d", domain.Unit)
		}
		return familySpecs[2], binaryParams(domain), nil
	case SignedInteger:
		if domain.Prefix != 0 || domain.Unit != 0 || domain.Charset != CharsetLegacy || domain.Width > 32 {
			return familySpec{}, nil, ErrUnsupportedDomain
		}
		return familySpecs[3], numericParams(domain.Width), nil
	case UnsignedInteger:
		if domain.Prefix != 0 || domain.Unit != 0 || domain.Charset != CharsetLegacy || domain.Width > 32 {
			return familySpec{}, nil, ErrUnsupportedDomain
		}
		return familySpecs[4], numericParams(domain.Width), nil
	case Decimal:
		if domain.Prefix != 0 || domain.Unit != 0 || domain.Charset != CharsetLegacy || domain.Width > 32 {
			return familySpec{}, nil, ErrUnsupportedDomain
		}
		return familySpecs[5], decimalParams(domain.Width, domain.Scale), nil
	default:
		return familySpec{}, nil, wrapCodecError(ErrUnsupportedDomain, "family %d", domain.Type)
	}
}

func textParams(domain Domain) []byte {
	return []byte{1, byte(domain.Prefix >> 24), byte(domain.Prefix >> 16), byte(domain.Prefix >> 8), byte(domain.Prefix), byte(PrefixCharacters)}
}

func binaryParams(domain Domain) []byte {
	return []byte{1, byte(domain.Prefix >> 24), byte(domain.Prefix >> 16), byte(domain.Prefix >> 8), byte(domain.Prefix), byte(PrefixBytes)}
}

func numericParams(width uint16) []byte { return []byte{1, byte(width >> 8), byte(width)} }

func decimalParams(width uint16, scale int16) []byte {
	return []byte{1, byte(width >> 8), byte(width), byte(uint16(scale) >> 8), byte(scale)}
}

func appendU32(dst []byte, value uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], value)
	return append(dst, b[:]...)
}

func appendU16(dst []byte, value uint16) []byte {
	var b [2]byte
	binary.BigEndian.PutUint16(b[:], value)
	return append(dst, b[:]...)
}

// EncodePart appends one complete v2 envelope containing a single part.
// `dst` may have a prefix, but the newly appended envelope is bounded by
// MaxKeyBytes.  Integer input must be exactly Domain.Width bytes in big-endian
// two's-complement (signed) or unsigned form. Decimal input is an ASCII
// decimal literal converted to Domain.Scale and normalized to a canonical
// sign/scale/coefficient payload.
func EncodePart(dst []byte, part Part) ([]byte, error) {
	return EncodeComposite(dst, []Part{part})
}

// EncodeComposite appends a framed composite identity.  A part count and
// length-delimited descriptors prevent concatenation ambiguity and preserve
// NULL versus an empty non-NULL value.
func EncodeComposite(dst []byte, parts []Part) ([]byte, error) {
	if len(parts) == 0 || len(parts) > MaxParts {
		return dst, wrapCodecError(ErrMalformedKey, "part count %d", len(parts))
	}
	start := len(dst)
	if start+7 > MaxKeyBytes {
		return dst, wrapCodecError(ErrMalformedKey, "key exceeds %d bytes", MaxKeyBytes)
	}
	dst = append(dst, magic...)
	dst = append(dst, CodecVersion)
	dst = appendU16(dst, uint16(len(parts)))
	for _, part := range parts {
		spec, params, err := domainSpec(part.Domain)
		if err != nil {
			return dst[:start], err
		}
		payload, err := normalize(part.Domain, spec, part.Value, part.Null)
		if err != nil {
			return dst[:start], err
		}
		if len(params) > maxParameter || len(payload) > maxPayload {
			return dst[:start], wrapCodecError(ErrMalformedKey, "encoded part is too large")
		}
		dst = append(dst, boolByte(part.Null), byte(part.Domain.Type))
		dst = appendU16(dst, spec.id)
		dst = appendU16(dst, uint16(len(params)))
		dst = append(dst, params...)
		dst = appendU32(dst, uint32(len(payload)))
		dst = append(dst, payload...)
		if len(dst)-start > MaxKeyBytes {
			return dst[:start], wrapCodecError(ErrMalformedKey, "key exceeds %d bytes", MaxKeyBytes)
		}
	}
	return dst, nil
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// Equal normalizes two non-NULL values under one fixed domain.  It is derived
// from the same normalization routine as EncodeComposite, so equality and
// persisted identity cannot drift.
func Equal(domain Domain, left, right []byte) (bool, error) {
	spec, _, err := domainSpec(domain)
	if err != nil {
		return false, err
	}
	a, err := normalize(domain, spec, left, false)
	if err != nil {
		return false, err
	}
	b, err := normalize(domain, spec, right, false)
	if err != nil {
		return false, err
	}
	return bytes.Equal(a, b), nil
}

// HashEncoded hashes the complete framed identity.  It is useful for bucket
// selection, but callers must compare the encoded bytes after a hash match.
func HashEncoded(encoded []byte) uint64 { return xxhash.Sum64(encoded) }

// ValidateEncoded checks framing, registry descriptors, lengths, and canonical
// flags without guessing a domain from payload bytes. It is intended for
// storage/replay readers before opening a v2 relation.
func ValidateEncoded(encoded []byte) error {
	if len(encoded) < 7 || len(encoded) > MaxKeyBytes || !bytes.Equal(encoded[:4], []byte(magic)) {
		return ErrMalformedKey
	}
	if encoded[4] != CodecVersion {
		return wrapCodecError(ErrMalformedKey, "codec version %d", encoded[4])
	}
	parts := int(binary.BigEndian.Uint16(encoded[5:7]))
	if parts == 0 || parts > MaxParts {
		return wrapCodecError(ErrMalformedKey, "part count %d", parts)
	}
	off := 7
	for i := 0; i < parts; i++ {
		if len(encoded)-off < 1+1+2+2+4 {
			return ErrMalformedKey
		}
		nullFlag, typ := encoded[off], TypeFamily(encoded[off+1])
		if nullFlag > 1 {
			return ErrMalformedKey
		}
		id := binary.BigEndian.Uint16(encoded[off+2 : off+4])
		paramLen := int(binary.BigEndian.Uint16(encoded[off+4 : off+6]))
		off += 6
		if len(encoded)-off < paramLen+4 {
			return ErrMalformedKey
		}
		params := encoded[off : off+paramLen]
		off += paramLen
		payloadLen64 := uint64(binary.BigEndian.Uint32(encoded[off : off+4]))
		off += 4
		if payloadLen64 > maxPayload || payloadLen64 > uint64(len(encoded)-off) {
			return ErrMalformedKey
		}
		payloadLen := int(payloadLen64)
		payload := encoded[off : off+payloadLen]
		off += payloadLen
		spec, ok := lookupFamily(id)
		if !ok || spec.typ != typ {
			return wrapCodecError(ErrMalformedKey, "unknown family %04x", id)
		}
		if !validateParams(spec, params) {
			return wrapCodecError(ErrMalformedKey, "non-canonical parameters for family %04x", id)
		}
		if nullFlag == 1 && payloadLen != 0 {
			return wrapCodecError(ErrMalformedKey, "NULL part has payload")
		}
		if nullFlag == 0 {
			if err := validatePayload(spec, params, payload); err != nil {
				return err
			}
		}
	}
	if off != len(encoded) {
		return wrapCodecError(ErrMalformedKey, "trailing bytes")
	}
	return nil
}

// HasNullPart reports whether a validated composite identity contains a NULL
// part. A NULL-bearing UNIQUE key is intentionally not a comparable sidecar
// identity under MatrixOne's existing SQL semantics: each row may carry its
// own NULL value without conflicting with another row. Callers that enforce
// uniqueness should therefore skip sidecar lookup/insert for such keys.
func HasNullPart(encoded []byte) (bool, error) {
	if err := ValidateEncoded(encoded); err != nil {
		return false, err
	}
	parts := int(binary.BigEndian.Uint16(encoded[5:7]))
	off := 7
	for i := 0; i < parts; i++ {
		if encoded[off] == 1 {
			return true, nil
		}
		paramLen := int(binary.BigEndian.Uint16(encoded[off+4 : off+6]))
		off += 6 + paramLen
		payloadLen := int(binary.BigEndian.Uint32(encoded[off : off+4]))
		off += 4 + payloadLen
	}
	return false, nil
}

func lookupFamily(id uint16) (familySpec, bool) {
	for _, spec := range familySpecs {
		if spec.id == id {
			return spec, true
		}
	}
	return familySpec{}, false
}

func validateParams(spec familySpec, params []byte) bool {
	switch spec.typ {
	case Text:
		return len(params) == 6 && params[0] == spec.params[0] && params[5] == byte(PrefixCharacters)
	case Binary:
		return len(params) == 6 && params[0] == spec.params[0] && params[5] == byte(PrefixBytes)
	case SignedInteger, UnsignedInteger:
		width := uint16(0)
		if len(params) == 3 {
			width = binary.BigEndian.Uint16(params[1:])
		}
		return len(params) == 3 && params[0] == spec.params[0] && width > 0 && width <= 32
	case Decimal:
		if len(params) != 5 || params[0] != spec.params[0] {
			return false
		}
		width := binary.BigEndian.Uint16(params[1:3])
		scale := int16(binary.BigEndian.Uint16(params[3:5]))
		return width > 0 && width <= 32 && scale >= 0
	default:
		return false
	}
}

func validGeneralPayload(payload []byte) bool {
	if len(payload)%4 != 0 {
		return false
	}
	for i := 0; i < len(payload); i += 4 {
		if binary.BigEndian.Uint32(payload[i:i+4]) > 0xffff {
			return false
		}
	}
	return true
}

func validatePayload(spec familySpec, params, payload []byte) error {
	switch spec.typ {
	case Text:
		if spec.charset == CharsetUTF8MB4Bin {
			if !utf8.Valid(payload) {
				return wrapCodecError(ErrMalformedKey, "malformed utf8-bin payload")
			}
		} else if !validGeneralPayload(payload) {
			return wrapCodecError(ErrMalformedKey, "malformed general-ci payload")
		}
	case SignedInteger, UnsignedInteger:
		if len(params) != 3 || len(payload) != int(binary.BigEndian.Uint16(params[1:])) {
			return wrapCodecError(ErrMalformedKey, "integer payload width")
		}
	case Decimal:
		if !validDecimalPayload(params, payload) {
			return wrapCodecError(ErrMalformedKey, "malformed decimal payload")
		}
	}
	return nil
}

func validDecimalPayload(params, payload []byte) bool {
	if len(params) != 5 || len(payload) < 9 || (payload[0] != 0 && payload[0] != 1) {
		return false
	}
	width := int(binary.BigEndian.Uint16(params[1:3]))
	targetScale := int16(binary.BigEndian.Uint16(params[3:5]))
	scale := int64(int32(binary.BigEndian.Uint32(payload[1:5])))
	if targetScale < 0 || scale < 0 || scale > int64(targetScale) {
		return false
	}
	coeffLen64 := uint64(binary.BigEndian.Uint32(payload[5:9]))
	if coeffLen64 > uint64(width) || coeffLen64 != uint64(len(payload)-9) {
		return false
	}
	coeffBytes := payload[9:]
	if len(coeffBytes) > 0 && coeffBytes[0] == 0 {
		return false
	}
	coeff := new(big.Int).SetBytes(coeffBytes)
	if coeff.Sign() == 0 {
		return payload[0] == 0 && scale == 0
	}
	if payload[0] == 1 && coeff.Sign() == 0 {
		return false
	}
	// normalizeDecimal removes every trailing decimal zero and decreases the
	// scale, so a non-zero coefficient divisible by ten is not canonical.
	return new(big.Int).Mod(coeff, big.NewInt(10)).Sign() != 0
}

func normalize(domain Domain, spec familySpec, value []byte, isNull bool) ([]byte, error) {
	if isNull {
		return nil, nil
	}
	switch spec.typ {
	case Text:
		return normalizeText(domain, spec, value)
	case Binary:
		return normalizeBinary(domain, value), nil
	case SignedInteger, UnsignedInteger:
		if len(value) != int(domain.Width) {
			return nil, wrapCodecError(ErrInvalidValue, "integer width %d, got %d", domain.Width, len(value))
		}
		return append([]byte(nil), value...), nil
	case Decimal:
		return normalizeDecimal(value, domain.Width, domain.Scale)
	default:
		return nil, ErrUnsupportedDomain
	}
}

func normalizeText(domain Domain, spec familySpec, value []byte) ([]byte, error) {
	if !utf8.Valid(value) {
		return nil, wrapCodecError(ErrInvalidValue, "invalid UTF-8")
	}
	if domain.Prefix > 0 {
		value = prefixRunes(value, domain.Prefix)
	}
	value = bytes.TrimRight(value, " ")
	if spec.id == 0x0002 {
		return append([]byte(nil), value...), nil
	}
	// One four-byte big-endian weight per rune is the frozen general-ci-v1
	// payload rule. The table itself is immutable and package-local.
	if len(value) > (MaxKeyBytes-32)/4 {
		return nil, wrapCodecError(ErrInvalidValue, "text payload exceeds key limit")
	}
	out := make([]byte, 0, len(value)*4)
	for len(value) > 0 {
		r, size := utf8.DecodeRune(value)
		if r == utf8.RuneError && size == 1 {
			return nil, wrapCodecError(ErrInvalidValue, "invalid UTF-8")
		}
		out = appendU32(out, utf8mb4GeneralCIWeight(r))
		value = value[size:]
	}
	return out, nil
}

func prefixRunes(value []byte, prefix uint32) []byte {
	if prefix == 0 {
		return value
	}
	count := uint32(0)
	for off := 0; off < len(value) && count < prefix; count++ {
		_, size := utf8.DecodeRune(value[off:])
		off += size
		if off == len(value) {
			return value
		}
	}
	// Re-scan to avoid returning a slice ending in the middle of a rune.
	off := 0
	for i := uint32(0); i < prefix && off < len(value); i++ {
		_, size := utf8.DecodeRune(value[off:])
		off += size
	}
	return value[:off]
}

func normalizeBinary(domain Domain, value []byte) []byte {
	if domain.Prefix > 0 && uint64(domain.Prefix) < uint64(len(value)) {
		value = value[:domain.Prefix]
	}
	return append([]byte(nil), value...)
}

func normalizeDecimal(value []byte, width uint16, targetScale int16) ([]byte, error) {
	if targetScale < 0 {
		return nil, wrapCodecError(ErrUnsupportedDomain, "negative decimal scale %d", targetScale)
	}
	s := strings.TrimSpace(string(value))
	if s == "" {
		return nil, wrapCodecError(ErrInvalidValue, "empty decimal")
	}
	neg := false
	if s[0] == '+' || s[0] == '-' {
		neg = s[0] == '-'
		s = s[1:]
	}
	if s == "" || strings.Count(s, ".") > 1 {
		return nil, wrapCodecError(ErrInvalidValue, "decimal syntax")
	}
	parts := strings.SplitN(s, ".", 2)
	whole, frac := parts[0], ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	if whole == "" {
		whole = "0"
	}
	if !allDigits(whole) || !allDigits(frac) {
		return nil, wrapCodecError(ErrInvalidValue, "decimal digits")
	}
	if len(frac) > math.MaxInt16 {
		return nil, wrapCodecError(ErrInvalidValue, "decimal scale exceeds int16")
	}
	inputScale := len(frac)
	coeffText := strings.TrimLeft(whole+frac, "0")
	if coeffText == "" {
		coeffText = "0"
		neg = false
	}
	coeff := new(big.Int)
	if _, ok := coeff.SetString(coeffText, 10); !ok {
		return nil, wrapCodecError(ErrInvalidValue, "decimal coefficient")
	}
	if inputScale > int(targetScale) {
		shift := inputScale - int(targetScale)
		div := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(shift)), nil)
		q, r := new(big.Int), new(big.Int)
		q.QuoRem(coeff, div, r)
		if r.Sign() != 0 {
			return nil, wrapCodecError(ErrInvalidValue, "decimal precision exceeds declared scale")
		}
		coeff = q
	} else if inputScale < int(targetScale) {
		mul := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(int(targetScale)-inputScale)), nil)
		coeff.Mul(coeff, mul)
	}
	scale := targetScale
	ten := big.NewInt(10)
	for coeff.Sign() != 0 && new(big.Int).Mod(coeff, ten).Sign() == 0 {
		if scale == math.MinInt16 {
			return nil, wrapCodecError(ErrInvalidValue, "decimal scale underflow")
		}
		coeff.Quo(coeff, ten)
		scale--
	}
	if coeff.Sign() == 0 {
		scale = 0
	}
	coeffBytes := coeff.Bytes()
	if len(coeffBytes) > int(width) {
		return nil, wrapCodecError(ErrInvalidValue, "decimal coefficient exceeds width %d", width)
	}
	out := make([]byte, 0, 1+4+4+len(coeffBytes))
	if neg {
		out = append(out, 1)
	} else {
		out = append(out, 0)
	}
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], uint32(int32(scale)))
	out = append(out, b[:]...)
	binary.BigEndian.PutUint32(b[:], uint32(len(coeffBytes)))
	out = append(out, b[:]...)
	out = append(out, coeffBytes...)
	return out, nil
}

func allDigits(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return false
		}
	}
	return true
}
