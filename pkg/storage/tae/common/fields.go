// Copyright 2022 Matrix Origin
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

package common

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

type Reprer interface {
	Repr() string
}

var StringerField = zap.Stringer
var AnyField = zap.Any

func TimestampField(val any) zap.Field               { return zap.Any("timestamp", val) }
func ReasonField(val string) zap.Field               { return zap.String("reason", val) }
func DurationField(val time.Duration) zap.Field      { return zap.Duration("duration", val) }
func OperationField(val string) zap.Field            { return zap.String("operation", val) }
func CountField(val int) zap.Field                   { return zap.Int("count", val) }
func IDField(val int) zap.Field                      { return zap.Int("id", val) }
func ContextField(format string, a ...any) zap.Field { return FormatFiled("ctx", format, a...) }
func EntityField(val any) zap.Field                  { return zap.Any("entity", val) }
func ExceptionField(val any) zap.Field               { return zap.Any("exception", val) }
func ErrorField(val error) zap.Field                 { return zap.Error(val) }
func NameSpaceField(val string) zap.Field            { return zap.Namespace(val) }
func OperandNameSpace() zap.Field                    { return zap.Namespace("operand") }
func ReprerField(key string, val Reprer) zap.Field   { return zap.String(key, val.Repr()) }
func OperandField(val any) zap.Field                 { return zap.Any("operand", val) }
func FormatFiled(key string, format string, a ...any) zap.Field {
	return zap.String(key, fmt.Sprintf(format, a...))
}

// func ObjectField(val any) zap.Field {
// 	return zap.Object(val.(zapcore.ObjectMarshaler))
// }
