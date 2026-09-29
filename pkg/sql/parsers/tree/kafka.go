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

// KafkaTableOption is one option of CREATE EXTERNAL TABLE ... ENGINE = KAFKA
// WITH (...). See docs/cn/kafka_exttab.md.
type KafkaTableOption struct {
	Key Identifier
	Val string
}

type KafkaTableOptions []*KafkaTableOption

func NewKafkaTableOption(key Identifier, value string) *KafkaTableOption {
	return &KafkaTableOption{Key: key, Val: value}
}

func (option *KafkaTableOption) Format(ctx *FmtCtx) {
	ctx.WriteString("\"")
	ctx.WriteString(string(option.Key))
	ctx.WriteString("\" = '")
	value := option.Val
	// none of the v1 options carry credentials, but redact secret-shaped keys
	// the same way datastream does so a future auth option cannot leak
	key := strings.ToLower(strings.ReplaceAll(string(option.Key), "_", ""))
	if strings.Contains(key, "password") || strings.Contains(key, "credential") ||
		strings.Contains(key, "token") || strings.Contains(key, "secret") ||
		strings.Contains(key, "apikey") {
		value = "<redacted>"
	}
	ctx.WriteString(strings.ReplaceAll(FormatString(value), "'", "''"))
	ctx.WriteByte('\'')
}

func (options KafkaTableOptions) Format(ctx *FmtCtx) {
	for i, option := range options {
		if i > 0 {
			ctx.WriteString(", ")
		}
		option.Format(ctx)
	}
}

// KafkaTableParam is the ENGINE = KAFKA clause of CREATE EXTERNAL TABLE.
type KafkaTableParam struct {
	Options KafkaTableOptions
}

func NewKafkaTableParam(options KafkaTableOptions) *KafkaTableParam {
	return &KafkaTableParam{Options: options}
}

func (param *KafkaTableParam) Format(ctx *FmtCtx) {
	ctx.WriteString("engine = kafka")
	if len(param.Options) > 0 {
		ctx.WriteString(" with (")
		param.Options.Format(ctx)
		ctx.WriteByte(')')
	}
}
