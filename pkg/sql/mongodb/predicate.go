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

package mongodb

import (
	"context"
	"slices"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type PredicateOp int32

const (
	PredicateInvalid PredicateOp = iota
	PredicateAnd
	PredicateEqual
	PredicateNotEqual
	PredicateLess
	PredicateLessEqual
	PredicateGreater
	PredicateGreaterEqual
	PredicateIn
	PredicateIsNull
	PredicateIsNotNull
)

// Predicate is a driver-neutral, serializable subset whose Mongo and MO
// semantics have been explicitly reviewed. Unsupported SQL remains residual.
type Predicate struct {
	Op       PredicateOp
	Path     string
	Value    any
	Values   []any
	Children []*Predicate
}

func (p *Predicate) Validate(ctx context.Context) error {
	if p == nil {
		return nil
	}
	if p.Op == PredicateAnd {
		if len(p.Children) == 0 {
			return moerr.NewInvalidInput(ctx, "MongoDB AND predicate requires children")
		}
		for _, child := range p.Children {
			if err := child.Validate(ctx); err != nil {
				return err
			}
		}
		return nil
	}
	if err := ValidateBSONPath(ctx, p.Path); err != nil {
		return err
	}
	switch p.Op {
	case PredicateEqual, PredicateNotEqual, PredicateLess, PredicateLessEqual,
		PredicateGreater, PredicateGreaterEqual, PredicateIsNull, PredicateIsNotNull:
		return nil
	case PredicateIn:
		if len(p.Values) == 0 {
			return moerr.NewInvalidInput(ctx, "MongoDB IN predicate cannot be empty")
		}
		return nil
	default:
		return moerr.NewInvalidInput(ctx, "unsupported MongoDB predicate")
	}
}

func ValidateBSONPath(ctx context.Context, path string) error {
	path = strings.TrimSpace(path)
	if path == "" || strings.HasPrefix(path, ".") || strings.HasSuffix(path, ".") || strings.Contains(path, "..") {
		return moerr.NewInvalidInput(ctx, "MongoDB column path must be a non-empty dotted scalar path")
	}
	for _, part := range strings.Split(path, ".") {
		if part == "" || strings.HasPrefix(part, "$") || strings.ContainsAny(part, "\x00*[]") {
			return moerr.NewInvalidInput(ctx, "MongoDB column path contains an unsupported component")
		}
	}
	return nil
}

func PredicateToBSON(ctx context.Context, p *Predicate) (bson.D, error) {
	if p == nil {
		return bson.D{}, nil
	}
	if err := p.Validate(ctx); err != nil {
		return nil, err
	}
	if p.Op == PredicateAnd {
		children := make(bson.A, 0, len(p.Children))
		for _, child := range p.Children {
			doc, err := PredicateToBSON(ctx, child)
			if err != nil {
				return nil, err
			}
			children = append(children, doc)
		}
		return bson.D{{Key: "$and", Value: children}}, nil
	}
	switch p.Op {
	case PredicateEqual:
		return bson.D{{Key: p.Path, Value: p.Value}}, nil
	case PredicateNotEqual:
		return bson.D{{Key: p.Path, Value: bson.D{{Key: "$ne", Value: p.Value}}}}, nil
	case PredicateLess:
		return comparisonDocument(p.Path, "$lt", p.Value), nil
	case PredicateLessEqual:
		return comparisonDocument(p.Path, "$lte", p.Value), nil
	case PredicateGreater:
		return comparisonDocument(p.Path, "$gt", p.Value), nil
	case PredicateGreaterEqual:
		return comparisonDocument(p.Path, "$gte", p.Value), nil
	case PredicateIn:
		return bson.D{{Key: p.Path, Value: bson.D{{Key: "$in", Value: bson.A(p.Values)}}}}, nil
	case PredicateIsNull:
		// MongoDB {field: null} intentionally matches both missing and BSON null,
		// exactly matching the MVP converter contract.
		return bson.D{{Key: p.Path, Value: nil}}, nil
	case PredicateIsNotNull:
		return bson.D{{Key: "$and", Value: bson.A{
			bson.D{{Key: p.Path, Value: bson.D{{Key: "$exists", Value: true}}}},
			bson.D{{Key: p.Path, Value: bson.D{{Key: "$ne", Value: nil}}}},
		}}}, nil
	default:
		return nil, moerr.NewInvalidInput(ctx, "unsupported MongoDB predicate")
	}
}

func comparisonDocument(path, op string, value any) bson.D {
	return bson.D{{Key: path, Value: bson.D{{Key: op, Value: value}}}}
}

func ProjectionDocument(columns []ColumnMapping) bson.D {
	projection := make(bson.D, 0, len(columns))
	for _, column := range columns {
		covered := false
		for _, selected := range projection {
			if selected.Key == column.Path || strings.HasPrefix(column.Path, selected.Key+".") {
				covered = true
				break
			}
		}
		if covered {
			continue
		}
		// MongoDB rejects an inclusion projection containing both a parent and
		// one of its descendants (for example payload and payload.value). If a
		// later mapping asks for the parent, it already carries every child the
		// converter needs, so replace the narrower entries with that parent.
		projection = slices.DeleteFunc(projection, func(selected bson.E) bool {
			return strings.HasPrefix(selected.Key, column.Path+".")
		})
		projection = append(projection, bson.E{Key: column.Path, Value: 1})
	}
	includeID := false
	for _, selected := range projection {
		if selected.Key == "_id" || strings.HasPrefix(selected.Key, "_id.") {
			includeID = true
			break
		}
	}
	if !includeID {
		projection = append(projection, bson.E{Key: "_id", Value: 0})
	}
	return projection
}
