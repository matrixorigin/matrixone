// Copyright 2021 - 2023 Matrix Origin
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

package moconnector

import (
	"context"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

const (
	SourceKafka string = "kafka"
	FormatJson  string = "json"
)

type StmtOpts map[string]string

type OptType uint16

const (
	OptTypeString OptType = iota
	OptTypeEnum
	OptTypeInteger
	OptTypeAddress
)

type OptValidator func(string) bool

type OptConstraint struct {
	Type      OptType
	Validator OptValidator
}

var (
	stringOpt = OptConstraint{
		Type: OptTypeString,
		Validator: func(s string) bool {
			return len(s) > 0
		},
	}
	integerOpt = OptConstraint{
		Type: OptTypeInteger,
		Validator: func(s string) bool {
			_, err := strconv.Atoi(s)
			return err == nil
		},
	}
	addressOpt = OptConstraint{
		Type: OptTypeAddress,
		Validator: func(s string) bool {
			ss := strings.Split(s, ":")
			if len(ss) != 2 || len(ss[0]) == 0 || len(ss[1]) == 0 {
				return false
			}
			_, err := strconv.Atoi(ss[1])
			return err == nil
		},
	}
)

func enumOpt(items ...string) OptConstraint {
	tc := OptConstraint{
		Type: OptTypeEnum,
		Validator: func(s string) bool {
			for _, item := range items {
				if item == s {
					return true
				}
			}
			return false
		},
	}
	return tc
}

const (
	OptConnectorType      = "type"
	OptConnectorServers   = "bootstrap.servers"
	OptConnectorTopic     = "topic"
	OptConnectorValue     = "value"
	OptConnectorPartition = "partition"
)

var ConnectorOptConstraint = map[string]OptConstraint{
	OptConnectorType:      enumOpt(SourceKafka),
	OptConnectorServers:   addressOpt,
	OptConnectorTopic:     stringOpt,
	OptConnectorValue:     enumOpt(FormatJson),
	OptConnectorPartition: integerOpt,
}

var ConnectorEssentialOpts = map[string]struct{}{
	OptConnectorType: {},
}

var ConnectorEssentialTypeOpts = map[string]map[string]struct{}{
	"kafka": {
		OptConnectorServers:   {},
		OptConnectorTopic:     {},
		OptConnectorPartition: {},
		OptConnectorValue:     {},
	},
}

func MakeStmtOpts(ctx context.Context, opts map[string]string) (StmtOpts, error) {
	if opts == nil {
		return StmtOpts{}, nil
	}
	for key := range ConnectorEssentialOpts {
		_, ok := opts[key]
		if !ok {
			return StmtOpts{}, moerr.NewErrLackOption(ctx, key)
		}
	}
	mapCopy := make(StmtOpts, len(opts))
	for key, value := range opts {
		optConstraint, ok := ConnectorOptConstraint[key]
		if !ok {
			return StmtOpts{}, moerr.NewErrUnsupportedOption(ctx, key)
		}
		if optConstraint.Validator != nil && !optConstraint.Validator(value) {
			return StmtOpts{}, moerr.NewErrInvalidValue(ctx, key, value)
		}
		mapCopy[key] = value
	}
	fields := ConnectorEssentialTypeOpts[opts[OptConnectorType]]
	for field := range fields {
		if _, ok := opts[field]; !ok {
			return StmtOpts{}, moerr.NewErrLackOption(ctx, field)
		}
	}
	return mapCopy, nil
}
