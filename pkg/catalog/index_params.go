// Copyright 2023 Matrix Origin
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

package catalog

import (
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
)

type IndexFullTextParserType uint16

const (
	IndexFullTextParserType_Invalid IndexFullTextParserType = iota
	IndexFullTextParserType_Ngram
	IndexFullTextParserType_JSONValue
	IndexFullTextParserType_Default
)

func (t IndexFullTextParserType) String() string {
	switch t {
	case IndexFullTextParserType_Default:
		return "default"
	case IndexFullTextParserType_Ngram:
		return "ngram"
	case IndexFullTextParserType_JSONValue:
		return "json_value"
	}
	return "invalid"
}

func (t IndexFullTextParserType) IsValid() bool {
	return t > IndexFullTextParserType_Invalid && t <= IndexFullTextParserType_JSONValue
}

func StringToIndexFullTextParserType(s string) IndexFullTextParserType {
	s = strings.ToLower(s)
	switch s {
	case "default":
		return IndexFullTextParserType_Default
	case "ngram":
		return IndexFullTextParserType_Ngram
	case "json_value":
		return IndexFullTextParserType_JSONValue
	default:
		return IndexFullTextParserType_Invalid
	}
}

type IndexParamType uint16

const (
	IndexParamType_Invalid IndexParamType = iota
	IndexParamType_FullText
)

func (t IndexParamType) String() string {
	switch t {
	case IndexParamType_FullText:
		return "fulltext"
	}
	return "invalid"
}

func (t IndexParamType) IsValid() bool {
	return t > IndexParamType_Invalid && t <= IndexParamType_FullText
}

const (
	IndexParamMagicNumber = 0x1234
)

var IndexParamMagicNumberBuf []byte
var IndexParamTypeFullTextBuf []byte

func init() {
	IndexParamMagicNumberBuf = types.EncodeFixed[uint16](uint16(IndexParamMagicNumber))
	IndexParamTypeFullTextBuf = types.EncodeFixed[uint16](uint16(IndexParamType_FullText))
}

// ------------------------[HEADER] IndexParams------------------------
const (
	IndexParams_MagicOff   = 0
	IndexParams_MagicLen   = 2
	IndexParams_TypeOff    = IndexParams_MagicOff + IndexParams_MagicLen
	IndexParams_TypeLen    = 2
	IndexParams_VersionOff = IndexParams_TypeOff + IndexParams_TypeLen
	IndexParams_VersionLen = 2
	IndexParams_HeaderLen  = IndexParams_MagicLen + IndexParams_TypeLen + IndexParams_VersionLen
)

// ------------------------[FULL TEXT V1 PARAMS] IndexParams------------------------
const (
	IndexParams_FullTextV1_ParserOff = IndexParams_HeaderLen
	IndexParams_FullTextV1_ParserLen = 2
	IndexParams_FullTextV1_Size      = IndexParams_FullTextV1_ParserOff + IndexParams_FullTextV1_ParserLen
)

type IndexParams []byte

func BuildIndexParamsFullTextV1(
	parser IndexFullTextParserType,
) IndexParams {
	buf := make([]byte, IndexParams_FullTextV1_Size)
	copy(buf, IndexParamMagicNumberBuf)
	copy(buf[IndexParams_TypeOff:], IndexParamTypeFullTextBuf)
	copy(buf[IndexParams_VersionOff:], types.EncodeFixed[uint16](1))
	copy(buf[IndexParams_FullTextV1_ParserOff:], types.EncodeFixed[uint16](uint16(parser)))
	return buf
}

func (params IndexParams) String() string {
	if len(params) < IndexParams_HeaderLen {
		return "invalid index params"
	}
	magic := types.DecodeFixed[uint16](params[:IndexParams_MagicLen])
	if magic != IndexParamMagicNumber {
		return fmt.Sprintf("invalid index params magic number: %x", magic)
	}
	paramType := IndexParamType(types.DecodeFixed[uint16](params[IndexParams_TypeOff:]))
	version := types.DecodeFixed[uint16](params[IndexParams_VersionOff:])
	switch paramType {
	case IndexParamType_FullText:
		parser := IndexFullTextParserType(types.DecodeFixed[uint16](params[IndexParams_FullTextV1_ParserOff:]))
		return fmt.Sprintf(
			"{param_type: %s, version: %d, parser: %s}",
			paramType.String(),
			version,
			parser.String(),
		)
	default:
		return fmt.Sprintf("invalid index params type: %s", paramType.String())
	}
}

func (params IndexParams) Type() IndexParamType {
	if len(params) < IndexParams_HeaderLen {
		return IndexParamType_Invalid
	}
	paramType := IndexParamType(types.DecodeFixed[uint16](params[IndexParams_TypeOff:]))
	return paramType
}

func (params IndexParams) Version() uint16 {
	if len(params) < IndexParams_HeaderLen {
		return 0
	}
	version := types.DecodeFixed[uint16](params[IndexParams_VersionOff:])
	return version
}
