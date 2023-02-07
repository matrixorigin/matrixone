// Copyright 2022 Matrix Origin
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

package internalExecutor

import "context"

/*
Define InternalExecutor interface here to avoid cycle dependency

The Impl of InternalExecutor is in frontend package
*/

type SessionOverrideOptions struct {
	Database   *string
	Username   *string
	IsInternal *bool
}

type OptsBuilder struct {
	opts *SessionOverrideOptions
}

func NewOptsBuilder() *OptsBuilder {
	return &OptsBuilder{
		opts: new(SessionOverrideOptions),
	}
}

func (s *OptsBuilder) Database(db string) *OptsBuilder {
	s.opts.Database = &db
	return s
}

func (s *OptsBuilder) Username(name string) *OptsBuilder {
	s.opts.Username = &name
	return s
}

func (s *OptsBuilder) Internal(b bool) *OptsBuilder {
	s.opts.IsInternal = &b
	return s
}

func (s *OptsBuilder) Finish() SessionOverrideOptions {
	return *s.opts
}

type InternalExecResult interface {
	Error() error
	ColumnCount() uint64
	Column(context.Context, uint64) (string, uint8, bool, error) // type refer: pkg/defines/type.go & func convertEngineTypeToMysqlType
	RowCount() uint64
	Row(context.Context, uint64) ([]interface{}, error)
	Value(context.Context, uint64, uint64) (interface{}, error)
	ValueByName(context.Context, uint64, string) (interface{}, error)
	StringValueByName(context.Context, uint64, string) (string, error)
	Float64ValueByName(context.Context, uint64, string) (float64, error)
}

type InternalExecutor interface {
	// exec sql without returning results set
	Exec(context.Context, string, SessionOverrideOptions) error
	// exec sql and return results set
	Query(context.Context, string, SessionOverrideOptions) InternalExecResult
	// override session for the executor scope
	ApplySessionOverride(SessionOverrideOptions)
}
