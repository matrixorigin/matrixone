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

package types

import (
	"bytes"
	"slices"

	"github.com/matrixorigin/matrixone/pkg/common/collation"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	// 无效原值与有效权重必须处于互不相交的比较域。
	validCollationKeyTag   byte = 0
	invalidCollationKeyTag byte = 1
)

// comparisonKeyDomain 只用于已校验的类型；SQL/持久化准入仍由各自边界负责。
// 同一 charset 的 revision 0/1 不能共享语义，binary 物理类型始终保留字节。
func comparisonKeyDomain(typ Type) (collation.Domain, bool) {
	if typ.CollationVersion != CollationVersionV1 || !isCollationStringType(typ.Oid) {
		return collation.Raw, false
	}
	switch typ.Charset {
	case CharsetUTF8MB4Bin:
		return collation.UTF8MB4Bin, true
	case CharsetUTF8:
		return collation.UTF8MB4GeneralCI, true
	case CharsetUTF8MB4UnicodeCI:
		return collation.UTF8MB4UnicodeCI, true
	case CharsetUTF8MB3UnicodeCI:
		return collation.UTF8MB3UnicodeCI, true
	default:
		return collation.Raw, false
	}
}

// HasCollationKey 判断已校验类型是否需要权重；不表示该类型已获 SQL/索引准入。
func HasCollationKey(typ Type) bool {
	_, ok := comparisonKeyDomain(typ)
	return ok
}

// CollationDomainForType 同时检查 identity、revision 和物理类型。
// 可表示但尚无执行实现的域不能静默退回 raw；合法 metadata 也不代表可发布。
func CollationDomainForType(typ Type) (collation.Domain, bool, error) {
	if err := typ.ValidateCollation(); err != nil {
		return collation.Raw, false, err
	}
	if !isCollationStringType(typ.Oid) || typ.CollationVersion == CollationVersionLegacy || typ.Charset == CharsetBinary {
		return collation.Raw, false, nil
	}
	domain, ok := comparisonKeyDomain(typ)
	if !ok {
		return collation.Raw, false, collation.ErrDomain
	}
	return domain, true, nil
}

// CollationKey 返回借用的原值或复用 scratch 的不透明权重，不修改原值。
// 输入与 scratch 不得重叠；需要保留的 key 由调用方复制。
func CollationKey(typ Type, scratch, value []byte) ([]byte, error) {
	domain, _, err := CollationDomainForType(typ)
	if err != nil {
		return nil, err
	}
	return domain.Key(scratch, value)
}

// ValidatePhysicalCollationType 在写入/打包之前检查完整 Type；公共执行支持
// 不等于索引格式准入。必须也校验空批次和全 NULL 输入，不能只依赖值校验。
func ValidatePhysicalCollationType(typ Type) error {
	_, _, err := physicalCollationDomain(typ)
	return err
}

func physicalCollationDomain(typ Type) (collation.Domain, bool, error) {
	domain, transformed, err := CollationDomainForType(typ)
	if err != nil {
		return domain, false, err
	}
	if transformed && !IsUnicodeCollation(typ.Charset) {
		return domain, false, moerr.NewNotSupportedNoCtx("physical keys for this collation revision are not enabled")
	}
	return domain, transformed, nil
}

// PhysicalCollationKey 是已获准的 UCA400 物理索引边界，不使用无效值 fallback。
// 公共比较能力的扩展不能顺带启用尚未闭合的物理 key 写入路径。
func PhysicalCollationKey(typ Type, value []byte) ([]byte, error) {
	domain, transformed, err := physicalCollationDomain(typ)
	if err != nil {
		return nil, err
	}
	if !transformed {
		return value, nil
	}
	key, err := domain.Key(nil, value)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 1+len(key))
	out[0] = validCollationKeyTag
	copy(out[1:], key)
	return out, nil
}

// CollationKeyOrOriginal 为已校验类型返回比较/hash 的统一身份。
// 无效 UTF-8/repertoire 原值带独立标签，不能与有效权重碰撞。
// 泛用 SERIAL 的原值往返契约不能使用此 key；物理写入使用严格入口。
func CollationKeyOrOriginal(typ Type, value []byte) []byte {
	domain, transformed := comparisonKeyDomain(typ)
	if !transformed {
		return value
	}
	if domain == collation.UTF8MB4GeneralCI || domain == collation.UTF8MB4Bin {
		return appendPADCollationKey(nil, domain, value)
	}
	key, err := domain.Key(nil, value)
	if err != nil {
		out := make([]byte, 1+len(value))
		out[0] = invalidCollationKeyTag
		copy(out[1:], value)
		return out
	}
	out := make([]byte, 1+len(key))
	out[0] = validCollationKeyTag
	copy(out[1:], key)
	return out
}

// CollationKeyOrOriginalSize 与带标签的 key 长度完全一致；C1 不分配中间 key。
func CollationKeyOrOriginalSize(typ Type, value []byte) int {
	domain, transformed := comparisonKeyDomain(typ)
	if !transformed {
		return len(value)
	}
	if domain == collation.UTF8MB4GeneralCI || domain == collation.UTF8MB4Bin {
		size, err := domain.PADKeySize(value)
		if err != nil {
			return 1 + len(value)
		}
		return 1 + size
	}
	return len(CollationKeyOrOriginal(typ, value))
}

// AppendCollationKeyOrOriginal 供已有计费 buffer/scratch 的消费者使用。
// dst 与 value 不得重叠；原值不会被修改，C1 只写入一次完整 key。
func AppendCollationKeyOrOriginal(dst []byte, typ Type, value []byte) []byte {
	domain, transformed := comparisonKeyDomain(typ)
	if transformed && (domain == collation.UTF8MB4GeneralCI || domain == collation.UTF8MB4Bin) {
		return appendPADCollationKey(dst, domain, value)
	}
	return append(dst, CollationKeyOrOriginal(typ, value)...)
}

func appendPADCollationKey(dst []byte, domain collation.Domain, value []byte) []byte {
	size, err := domain.PADKeySize(value)
	if err != nil {
		dst = append(dst, invalidCollationKeyTag)
		return append(dst, value...)
	}
	// PADKeySize 已检查 payload 上界（4*n+1），可表示输入下再加标签不会溢出。
	dst = slices.Grow(dst, size+1)
	dst = append(dst, validCollationKeyTag)
	key, _ := domain.Key(dst[len(dst):], value) // 与 size 共用已验证的输入和域。
	return dst[:len(dst)+len(key)]
}

// ValidateCollationValue 只校验支持域的值编码，不构造 key。通用向量 codec
// 仍允许合法但禁用的 metadata 往返；结构校验/生产准入由各自边界负责。
func ValidateCollationValue(typ Type, value []byte) error {
	domain, transformed := comparisonKeyDomain(typ)
	if !transformed {
		return nil
	}
	return domain.ValidateValue(value)
}

func isCollationStringType(oid T) bool {
	switch oid {
	case T_char, T_varchar, T_blob, T_text:
		return true
	default:
		return false
	}
}

// CompareStringValues 对已解析到同一域的值比较。legacy 保持原有 CHAR
// 行为；native 的比较和 hash 使用同一带标签权重，原值只用于结果投影。
func CompareStringValues(typ Type, left, right []byte) int {
	if !HasCollationKey(typ) {
		if typ.Oid == T_char {
			return bytes.Compare(bytes.TrimRight(left, " "), bytes.TrimRight(right, " "))
		}
		return bytes.Compare(left, right)
	}
	return bytes.Compare(CollationKeyOrOriginal(typ, left), CollationKeyOrOriginal(typ, right))
}

// CompareStringOrderValues 仅用于 SQL 排序。legacy CHAR 的历史 raw 顺序
// 不变；storage 的物理排序不能因 SQL 比较域扩展而被重新解释。
func CompareStringOrderValues(typ Type, left, right []byte) int {
	if !HasCollationKey(typ) {
		return bytes.Compare(left, right)
	}
	return CompareStringValues(typ, left, right)
}
