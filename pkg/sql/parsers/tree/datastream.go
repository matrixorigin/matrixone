// Copyright 2026 Matrix Origin
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

package tree

import "strings"

type DataStreamOption struct {
	Key Identifier
	Val string
}

type DataStreamOptions []*DataStreamOption

func NewDataStreamOption(key Identifier, value string) *DataStreamOption {
	return &DataStreamOption{Key: key, Val: value}
}

func (option *DataStreamOption) Format(ctx *FmtCtx) {
	ctx.WriteString("\"")
	ctx.WriteString(string(option.Key))
	ctx.WriteString("\" = '")
	value := option.Val
	key := strings.ToLower(strings.ReplaceAll(string(option.Key), "_", ""))
	if strings.Contains(key, "password") || strings.Contains(key, "credential") ||
		strings.Contains(key, "token") || strings.Contains(key, "secret") ||
		strings.Contains(key, "apikey") {
		value = "<redacted>"
	}
	ctx.WriteString(strings.ReplaceAll(FormatString(value), "'", "''"))
	ctx.WriteByte('\'')
}

func (options DataStreamOptions) Format(ctx *FmtCtx) {
	for i, option := range options {
		if i > 0 {
			ctx.WriteString(", ")
		}
		option.Format(ctx)
	}
}

type DataStreamTableParam struct {
	Options DataStreamOptions
}

func NewDataStreamTableParam(options DataStreamOptions) *DataStreamTableParam {
	return &DataStreamTableParam{Options: options}
}

func (param *DataStreamTableParam) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = datastream")
	if len(param.Options) > 0 {
		ctx.WriteString(" with (")
		param.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}
