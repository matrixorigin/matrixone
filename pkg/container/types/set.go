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

package types

import (
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const MaxSetMembers = 64

var setDescriptorCache sync.Map

// EncodeSetValues keeps an empty member distinct from absent SET metadata.
// The reserved comma cannot encode a valid legacy domain: members cannot
// contain commas, and two empty members are rejected as duplicates.
func EncodeSetValues(values []string) (string, error) {
	values, err := NormalizeSetValues(values)
	if err != nil {
		return "", err
	}
	if len(values) == 1 && values[0] == "" {
		return ",", nil
	}
	return strings.Join(values, ","), nil
}

// DecodeSetValues decodes domain metadata, not a SET row's display value.
func DecodeSetValues(value string) []string {
	switch value {
	case "":
		return nil
	case ",":
		return []string{""}
	default:
		return strings.Split(value, ",")
	}
}

func SetValidBitmap(definition string) (uint64, error) {
	descriptor, err := getSetDescriptor(definition)
	if err != nil {
		return 0, err
	}
	return descriptor.validBitmap, nil
}

func NormalizeSetValues(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, moerr.NewInvalidInputNoCtx("set type length err")
	}
	if len(values) > MaxSetMembers {
		return nil, moerr.NewInvalidInputNoCtxf("set type out of max length %d", MaxSetMembers)
	}

	normalized := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if strings.Contains(value, ",") {
			return nil, moerr.NewInvalidInputNoCtxf("set member %q contains ','", value)
		}

		trimmed := strings.TrimRight(value, " ")
		key := strings.ToLower(trimmed)
		if _, ok := normalized[key]; ok {
			return nil, moerr.NewInvalidInputNoCtxf("duplicate set member %q", value)
		}

		normalized[key] = struct{}{}
		result = append(result, trimmed)
	}
	return result, nil
}

func ParseSet(setStr string, name string) (uint64, error) {
	descriptor, err := getSetDescriptor(setStr)
	if err != nil {
		return 0, err
	}

	setBits, matched := descriptor.parseName(name)
	if matched {
		return setBits, nil
	}

	number, err := strconv.ParseUint(name, 0, 64)
	if err == nil {
		return descriptor.parseValue(number)
	}

	return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL set failed: item %s is not in set [%s]", name, strings.Join(descriptor.values, ","))
}

// MatchSetName reports whether name is a list of member names of the SET
// definition, exactly as ParseSet matches names, without building an error.
// Numeric bitmap input is not considered.
func MatchSetName(setStr string, name string) bool {
	descriptor, err := getSetDescriptor(setStr)
	if err != nil {
		return false
	}
	_, matched := descriptor.parseName(name)
	return matched
}

func ParseSetValue(setStr string, bits uint64) (uint64, error) {
	descriptor, err := getSetDescriptor(setStr)
	if err != nil {
		return 0, err
	}
	return descriptor.parseValue(bits)
}

func ParseSetIndex(setStr string, bits uint64) (string, error) {
	descriptor, err := getSetDescriptor(setStr)
	if err != nil {
		return "", err
	}
	return descriptor.bitsToString(bits)
}

type setDescriptor struct {
	values             []string
	valueToBit         map[string]uint64
	normalizedToOrigin map[string]string
	validBitmap        uint64
}

func getSetDescriptor(setStr string) (*setDescriptor, error) {
	if descriptor, ok := setDescriptorCache.Load(setStr); ok {
		return descriptor.(*setDescriptor), nil
	}

	descriptor, err := buildSetDescriptor(setStr)
	if err != nil {
		return nil, err
	}

	actual, _ := setDescriptorCache.LoadOrStore(setStr, descriptor)
	return actual.(*setDescriptor), nil
}

func buildSetDescriptor(setStr string) (*setDescriptor, error) {
	if len(setStr) == 0 {
		return nil, moerr.NewInternalErrorNoCtxf("convert to MySQL set failed: set define is empty %v", setStr)
	}

	values, err := NormalizeSetValues(DecodeSetValues(setStr))
	if err != nil {
		return nil, err
	}

	valueToBit := make(map[string]uint64, len(values))
	normalizedToOrigin := make(map[string]string, len(values))
	var validBitmap uint64
	for i, value := range values {
		bit := uint64(1) << uint(i)
		valueToBit[value] = bit
		normalizedToOrigin[strings.ToLower(value)] = value
		validBitmap |= bit
	}

	return &setDescriptor{
		values:             values,
		valueToBit:         valueToBit,
		normalizedToOrigin: normalizedToOrigin,
		validBitmap:        validBitmap,
	}, nil
}

func (d *setDescriptor) parseName(name string) (uint64, bool) {
	if len(name) == 0 {
		return 0, true
	}

	// Walk the comma-separated items in place (same items as strings.Split)
	// so a member-name lookup does not allocate.
	setBits := uint64(0)
	for {
		item, rest, more := strings.Cut(name, ",")
		normalized := strings.ToLower(strings.TrimRight(item, " "))
		origin, ok := d.normalizedToOrigin[normalized]
		if !ok {
			return 0, false
		}
		setBits |= d.valueToBit[origin]
		if !more {
			return setBits, true
		}
		name = rest
	}
}

func (d *setDescriptor) parseValue(bits uint64) (uint64, error) {
	if bits&^d.validBitmap != 0 {
		return 0, moerr.NewInternalErrorNoCtxf("convert to MySQL set failed: value %d overflow set boundary %d", bits, d.validBitmap)
	}
	return bits, nil
}

func (d *setDescriptor) bitsToString(bits uint64) (string, error) {
	if bits, err := d.parseValue(bits); err != nil {
		return "", err
	} else {
		builder := strings.Builder{}
		for idx, value := range d.values {
			mask := uint64(1) << uint(idx)
			if bits&mask == 0 {
				continue
			}

			if builder.Len() > 0 {
				builder.WriteByte(',')
			}
			builder.WriteString(value)
		}
		return builder.String(), nil
	}
}
