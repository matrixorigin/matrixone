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

package logutil

import (
	"go.uber.org/zap"
)

func ConnectionIdField(val uint32) zap.Field { return zap.Uint32("connection_id", val) }
func QueryField(val string) zap.Field        { return zap.String("query", val) }
func StatementField(val string) zap.Field    { return zap.String("statement", val) }
func VarsField(val string) zap.Field         { return zap.String("vars", val) }
func StatusField(val string) zap.Field       { return zap.String("status", val) }
func ErrorField(err error) zap.Field         { return zap.Error(err) }
func TableField(val string) zap.Field        { return zap.String("table", val) } // table name
func PathField(val string) zap.Field         { return zap.String("path", val) }

func NoReportFiled() zap.Field { return zap.Bool(MOInternalFiledKeyNoopReport, true) }
func Discardable() zap.Field   { return zap.Bool(MOInternalFiledKeyDiscardable, true) }
