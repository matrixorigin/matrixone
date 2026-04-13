// Copyright 2025 Matrix Origin
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

package fulltext

import (
	"encoding/json"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/bytejson"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/monlp/tokenizer"
)

type IndexToken struct {
	Word string
	Pos  int32
}

type IndexValue struct {
	Text string
	Raw  []byte
	Type types.T
}

type DatalinkTextResolver func(string) (string, error)

func TokenizeIndexValues(
	param FullTextParserParam,
	values []IndexValue,
	resolveDatalink DatalinkTextResolver,
) ([]IndexToken, error) {
	switch param.Parser {
	case "", "ngram", "default":
		return tokenizeDefaultValues(values, resolveDatalink)
	case "json":
		return tokenizeJSONValues(values, false)
	case "json_value":
		return tokenizeJSONValues(values, true)
	default:
		return nil, moerr.NewInternalErrorNoCtx("invalid fulltext parser")
	}
}

func tokenizeDefaultValues(values []IndexValue, resolveDatalink DatalinkTextResolver) ([]IndexToken, error) {
	var builder strings.Builder
	for i, value := range values {
		if i > 0 {
			builder.WriteByte('\n')
		}
		text, err := materializeIndexValueText(value, resolveDatalink)
		if err != nil {
			return nil, err
		}
		builder.WriteString(text)
	}
	return appendSimpleTokens(nil, builder.String(), 0)
}

func materializeIndexValueText(value IndexValue, resolveDatalink DatalinkTextResolver) (string, error) {
	text := value.Text
	if text == "" && len(value.Raw) > 0 {
		text = string(value.Raw)
	}
	if value.Type != types.T_datalink {
		return text, nil
	}
	if resolveDatalink == nil {
		return "", moerr.NewInternalErrorNoCtx("datalink resolver is required for fulltext tokenization")
	}
	return resolveDatalink(text)
}

func tokenizeJSONValues(values []IndexValue, keepWholeValue bool) ([]IndexToken, error) {
	tokens := make([]IndexToken, 0, 64)
	joffset := int32(0)
	for _, value := range values {
		raw := value.Raw
		if len(raw) == 0 {
			raw = []byte(value.Text)
		}
		bj, err := decodeIndexValueJSON(raw, value.Type)
		if err != nil {
			return nil, err
		}
		voffset := int32(0)
		for token := range bj.TokenizeValue(false) {
			slen := token.TokenBytes[0]
			word := string(token.TokenBytes[1 : slen+1])
			if keepWholeValue {
				tokens = append(tokens, IndexToken{
					Word: word,
					Pos:  joffset + voffset,
				})
			} else {
				tokens, err = appendSimpleTokens(tokens, word, joffset+voffset)
				if err != nil {
					return nil, err
				}
			}
			voffset += int32(slen)
		}
		joffset += int32(len(raw))
	}
	return tokens, nil
}

func decodeIndexValueJSON(raw []byte, typ types.T) (bytejson.ByteJson, error) {
	var bj bytejson.ByteJson
	if typ == types.T_json {
		return bj, bj.Unmarshal(raw)
	}
	return bj, json.Unmarshal(raw, &bj)
}

func appendSimpleTokens(tokens []IndexToken, content string, basePos int32) ([]IndexToken, error) {
	tok, err := tokenizer.NewSimpleTokenizer([]byte(content))
	if err != nil {
		return nil, err
	}
	for t := range tok.Tokenize() {
		slen := t.TokenBytes[0]
		tokens = append(tokens, IndexToken{
			Word: string(t.TokenBytes[1 : slen+1]),
			Pos:  basePos + t.BytePos,
		})
	}
	return tokens, nil
}
