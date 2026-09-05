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

package function

import (
	"context"
	"errors"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/xeipuuv/gojsonschema"
)

const mysqlJSONSchemaMaxDepth = 100

const (
	mysqlJSONSchemaExternalRefReason = "only local JSON Schema $ref values \"#\" and \"#/...\" are supported"
	mysqlJSONSchemaRefStringReason   = "JSON Schema $ref must be a string"
	mysqlJSONSchemaRefSyntaxReason   = "invalid local JSON Schema $ref"
	mysqlJSONSchemaRefTargetReason   = "local JSON Schema $ref target does not exist"
	mysqlJSONSchemaRefCycleReason    = "cyclic local JSON Schema $ref is not allowed"
	mysqlJSONSchemaDepthReason       = "JSON Schema/JSON document nesting depth exceeds 100"
	mysqlJSONSchemaExpansionReason   = "JSON Schema expansion depth exceeds 100"
)

var (
	errMySQLJSONSchemaExternalLoad error = mysqlJSONSchemaSentinel("external JSON Schema reference loading is disabled")
	errMySQLJSONSchemaExternalRef  error = mysqlJSONSchemaSentinel(mysqlJSONSchemaExternalRefReason)
	errMySQLJSONSchemaRefSyntax    error = mysqlJSONSchemaSentinel(mysqlJSONSchemaRefSyntaxReason)
	errMySQLJSONSchemaRefTarget    error = mysqlJSONSchemaSentinel(mysqlJSONSchemaRefTargetReason)
	errMySQLJSONSchemaRefCycle     error = mysqlJSONSchemaSentinel(mysqlJSONSchemaRefCycleReason)
	errMySQLJSONSchemaDepth        error = mysqlJSONSchemaSentinel(mysqlJSONSchemaDepthReason)
	errMySQLJSONSchemaExpansion    error = mysqlJSONSchemaSentinel(mysqlJSONSchemaExpansionReason)
)

type mysqlJSONSchemaSentinel string

func (e mysqlJSONSchemaSentinel) Error() string {
	return string(e)
}

// mysqlDraft4RootLoader keeps gojsonschema's raw loader for the in-memory
// document, while replacing its factory so a missed external reference can
// never reach the file or HTTP loaders.
type mysqlDraft4RootLoader struct {
	gojsonschema.JSONLoader
	factory *mysqlDraft4DenyFactory
}

func (l *mysqlDraft4RootLoader) LoaderFactory() gojsonschema.JSONLoaderFactory {
	return l.factory
}

type mysqlDraft4DenyFactory struct {
	calls int
}

func (f *mysqlDraft4DenyFactory) New(source string) gojsonschema.JSONLoader {
	f.calls++
	return &mysqlDraft4DenyLoader{
		JSONLoader: gojsonschema.NewReferenceLoader(source),
	}
}

type mysqlDraft4DenyLoader struct {
	gojsonschema.JSONLoader
}

func (l *mysqlDraft4DenyLoader) LoadJSON() (interface{}, error) {
	return nil, errMySQLJSONSchemaExternalLoad
}

type mysqlJSONSchemaNode struct {
	value        any
	containment  []string
	edges        []string
	baseExternal bool
}

type mysqlJSONSchemaIndex struct {
	nodes          map[string]*mysqlJSONSchemaNode
	values         map[string]any
	nodeVisits     int
	refOccurrences int
	edgeVisits     int
	refEdges       int
}

type mysqlJSONSchemaRef struct {
	target string
}

type mysqlEffectiveSchemaPending struct {
	pointer string
	value   any
}

// mysqlAnalyzeDraft4Schema performs all local-reference checks before
// gojsonschema sees the document. Effective $ref values are rewritten to
// canonical JSON Pointers in place.
func mysqlAnalyzeDraft4Schema(ctx context.Context, fnName string, schema any) error {
	index, err := mysqlIndexSchemaJSON(ctx, schema)
	if err != nil {
		return mysqlSchemaRefError(ctx, fnName, err)
	}
	refs, err := mysqlScanSchemaStringRefs(ctx, index)
	if err != nil {
		return mysqlSchemaRefError(ctx, fnName, err)
	}
	effectiveTargets, err := mysqlScanEffectiveSchemaRefs(ctx, fnName, index, refs)
	if err != nil {
		return err
	}
	for target := range effectiveTargets {
		if node, ok := index.nodes[target]; ok {
			if object, ok := node.value.(map[string]any); ok {
				normalizeMySQLDraft4Schema(object)
			}
		}
	}
	if err := mysqlValidateSchemaRefGraph(ctx, index); err != nil {
		return mysqlSchemaRefError(ctx, fnName, err)
	}
	return nil
}

func mysqlSchemaRefError(ctx context.Context, fnName string, err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	switch err.Error() {
	case mysqlJSONSchemaExternalRefReason:
		return moerr.NewNotSupportedf(ctx, "%s: %s", fnName, err.Error())
	case mysqlJSONSchemaRefStringReason, mysqlJSONSchemaRefSyntaxReason,
		mysqlJSONSchemaRefTargetReason, mysqlJSONSchemaRefCycleReason,
		mysqlJSONSchemaDepthReason, mysqlJSONSchemaExpansionReason:
		return moerr.NewInvalidArg(ctx, fnName, err.Error())
	default:
		return moerr.NewInvalidArg(ctx, fnName, err.Error())
	}
}

func mysqlIndexSchemaJSON(ctx context.Context, schema any) (*mysqlJSONSchemaIndex, error) {
	index := &mysqlJSONSchemaIndex{
		nodes:  make(map[string]*mysqlJSONSchemaNode),
		values: make(map[string]any),
	}
	type pending struct {
		pointer      string
		value        any
		depth        int
		baseExternal bool
	}
	stack := []pending{{pointer: "#", value: schema, depth: 1}}
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		last := len(stack) - 1
		item := stack[last]
		stack = stack[:last]
		if item.depth > mysqlJSONSchemaMaxDepth {
			return nil, errMySQLJSONSchemaDepth
		}
		baseExternal := item.baseExternal
		if object, ok := item.value.(map[string]any); ok {
			baseExternal = mysqlSchemaObjectHasExternalBase(object, baseExternal)
		}
		node := &mysqlJSONSchemaNode{
			value:        item.value,
			baseExternal: baseExternal,
		}
		index.values[item.pointer] = item.value
		if !mysqlJSONSchemaIsContainer(item.value) {
			continue
		}
		index.nodes[item.pointer] = node
		index.nodeVisits++

		switch value := item.value.(type) {
		case map[string]any:
			keys := make([]string, 0, len(value))
			for key := range value {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			for i := len(keys) - 1; i >= 0; i-- {
				key := keys[i]
				pointer := mysqlJSONPointerChild(item.pointer, key)
				child := value[key]
				if mysqlJSONSchemaIsContainer(child) {
					node.containment = append(node.containment, pointer)
					index.edgeVisits++
				}
				index.values[pointer] = child
				if !mysqlJSONSchemaIsContainer(child) {
					continue
				}
				stack = append(stack, pending{
					pointer:      pointer,
					value:        child,
					depth:        item.depth + mysqlJSONSchemaDepthStep(child),
					baseExternal: baseExternal,
				})
			}
		case []any:
			for i := len(value) - 1; i >= 0; i-- {
				pointer := mysqlJSONPointerChild(item.pointer, strconv.Itoa(i))
				child := value[i]
				if mysqlJSONSchemaIsContainer(child) {
					node.containment = append(node.containment, pointer)
					index.edgeVisits++
				}
				index.values[pointer] = child
				if !mysqlJSONSchemaIsContainer(child) {
					continue
				}
				stack = append(stack, pending{
					pointer:      pointer,
					value:        child,
					depth:        item.depth + mysqlJSONSchemaDepthStep(child),
					baseExternal: baseExternal,
				})
			}
		}
	}

	// The stack above records children in reverse insertion order. Rebuild the
	// containment edge lists from sorted pointers for deterministic traversal.
	for pointer, node := range index.nodes {
		if object, ok := node.value.(map[string]any); ok {
			keys := make([]string, 0, len(object))
			for key := range object {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			node.containment = node.containment[:0]
			for _, key := range keys {
				if mysqlJSONSchemaIsContainer(object[key]) {
					node.containment = append(node.containment, mysqlJSONPointerChild(pointer, key))
				}
			}
		} else if array, ok := node.value.([]any); ok {
			node.containment = node.containment[:0]
			for i := range array {
				if mysqlJSONSchemaIsContainer(array[i]) {
					node.containment = append(node.containment, mysqlJSONPointerChild(pointer, strconv.Itoa(i)))
				}
			}
		}
		node.edges = append(node.edges, node.containment...)
	}
	return index, nil
}

func mysqlJSONSchemaIsContainer(value any) bool {
	switch value.(type) {
	case map[string]any, []any:
		return true
	default:
		return false
	}
}

func mysqlJSONSchemaDepthStep(value any) int {
	if mysqlJSONSchemaIsContainer(value) {
		return 1
	}
	return 0
}

func mysqlSchemaObjectHasExternalBase(object map[string]any, inherited bool) bool {
	baseExternal := inherited
	for _, key := range []string{"id", "$id"} {
		value, ok := object[key].(string)
		if !ok || value == "" {
			continue
		}
		if value != "#" && !strings.HasPrefix(value, "#/") {
			baseExternal = true
		}
	}
	return baseExternal
}

func mysqlJSONPointerChild(parent, token string) string {
	return parent + "/" + strings.NewReplacer("~", "~0", "/", "~1").Replace(token)
}

func mysqlJSONPointerPath(parent string, tokens ...string) string {
	for _, token := range tokens {
		parent = mysqlJSONPointerChild(parent, token)
	}
	return parent
}

func mysqlScanSchemaStringRefs(ctx context.Context, index *mysqlJSONSchemaIndex) (map[string]mysqlJSONSchemaRef, error) {
	refs := make(map[string]mysqlJSONSchemaRef)
	var malformed, missing bool
	pointers := make([]string, 0, len(index.nodes))
	for pointer := range index.nodes {
		pointers = append(pointers, pointer)
	}
	sort.Strings(pointers)
	for _, pointer := range pointers {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		object, ok := index.nodes[pointer].value.(map[string]any)
		if !ok {
			continue
		}
		value, exists := object["$ref"]
		if !exists {
			continue
		}
		index.refOccurrences++
		stringValue, ok := value.(string)
		if !ok {
			continue
		}
		if index.nodes[pointer].baseExternal {
			return nil, errMySQLJSONSchemaExternalRef
		}
		ref, err := mysqlResolveLocalSchemaRef(index, stringValue)
		if err != nil {
			if errors.Is(err, errMySQLJSONSchemaExternalRef) {
				return nil, errMySQLJSONSchemaExternalRef
			}
			if errors.Is(err, errMySQLJSONSchemaRefTarget) {
				missing = true
			} else {
				malformed = true
			}
			continue
		}
		refs[pointer] = ref
	}
	if malformed {
		return nil, errMySQLJSONSchemaRefSyntax
	}
	if missing {
		return nil, errMySQLJSONSchemaRefTarget
	}
	return refs, nil
}

func mysqlResolveLocalSchemaRef(index *mysqlJSONSchemaIndex, raw string) (mysqlJSONSchemaRef, error) {
	if raw == "#" {
		return mysqlJSONSchemaRef{target: "#"}, nil
	}
	if !strings.HasPrefix(raw, "#/") {
		return mysqlJSONSchemaRef{}, errMySQLJSONSchemaExternalRef
	}
	decoded, err := mysqlStrictPercentDecode(raw[1:])
	if err != nil || !strings.HasPrefix(decoded, "/") {
		return mysqlJSONSchemaRef{}, errMySQLJSONSchemaRefSyntax
	}
	rawTokens := strings.Split(decoded[1:], "/")
	tokens := make([]string, len(rawTokens))
	for i, token := range rawTokens {
		tokens[i], err = mysqlDecodeJSONPointerToken(token)
		if err != nil {
			return mysqlJSONSchemaRef{}, errMySQLJSONSchemaRefSyntax
		}
	}

	pointer := "#"
	current, currentExists := index.values[pointer]
	for _, token := range tokens {
		if !currentExists {
			return mysqlJSONSchemaRef{}, errMySQLJSONSchemaRefTarget
		}
		switch value := current.(type) {
		case map[string]any:
			child, ok := value[token]
			if !ok {
				return mysqlJSONSchemaRef{}, errMySQLJSONSchemaRefTarget
			}
			pointer = mysqlJSONPointerChild(pointer, token)
			current, currentExists = index.values[pointer]
			_ = child
		case []any:
			if !mysqlValidJSONPointerArrayIndex(token) {
				return mysqlJSONSchemaRef{}, errMySQLJSONSchemaRefSyntax
			}
			position, parseErr := strconv.ParseUint(token, 10, 64)
			if parseErr != nil || position >= uint64(len(value)) {
				return mysqlJSONSchemaRef{}, errMySQLJSONSchemaRefTarget
			}
			pointer = mysqlJSONPointerChild(pointer, token)
			current, currentExists = index.values[pointer]
		default:
			return mysqlJSONSchemaRef{}, errMySQLJSONSchemaRefTarget
		}
	}
	return mysqlJSONSchemaRef{target: pointer}, nil
}

func mysqlStrictPercentDecode(value string) (string, error) {
	for i := 0; i < len(value); i++ {
		if value[i] != '%' {
			continue
		}
		if i+2 >= len(value) || !mysqlHex(value[i+1]) || !mysqlHex(value[i+2]) {
			return "", errMySQLJSONSchemaRefSyntax
		}
		i += 2
	}
	decoded, err := url.PathUnescape(value)
	if err != nil || !utf8.ValidString(decoded) {
		return "", errMySQLJSONSchemaRefSyntax
	}
	return decoded, nil
}

func mysqlHex(value byte) bool {
	return value >= '0' && value <= '9' || value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
}

func mysqlDecodeJSONPointerToken(token string) (string, error) {
	var builder strings.Builder
	for i := 0; i < len(token); i++ {
		if token[i] != '~' {
			builder.WriteByte(token[i])
			continue
		}
		if i+1 >= len(token) || (token[i+1] != '0' && token[i+1] != '1') {
			return "", errMySQLJSONSchemaRefSyntax
		}
		if token[i+1] == '0' {
			builder.WriteByte('~')
		} else {
			builder.WriteByte('/')
		}
		i++
	}
	return builder.String(), nil
}

func mysqlValidJSONPointerArrayIndex(value string) bool {
	if value == "0" {
		return true
	}
	if value == "" || value[0] < '1' || value[0] > '9' {
		return false
	}
	for i := 1; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	return true
}

func mysqlScanEffectiveSchemaRefs(ctx context.Context, fnName string, index *mysqlJSONSchemaIndex, refs map[string]mysqlJSONSchemaRef) (map[string]struct{}, error) {
	stack := []mysqlEffectiveSchemaPending{{pointer: "#", value: index.nodes["#"].value}}
	visited := make(map[string]struct{}, len(index.nodes))
	effectiveTargets := make(map[string]struct{})
	for len(stack) > 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		last := len(stack) - 1
		item := stack[last]
		stack = stack[:last]
		if _, ok := visited[item.pointer]; ok {
			continue
		}
		visited[item.pointer] = struct{}{}
		object, ok := item.value.(map[string]any)
		if !ok {
			continue
		}

		if value, exists := object["$ref"]; exists {
			stringValue, isString := value.(string)
			if !isString {
				return nil, moerr.NewInvalidArg(ctx, fnName, mysqlJSONSchemaRefStringReason)
			}
			ref, ok := refs[item.pointer]
			if !ok {
				return nil, moerr.NewInvalidArg(ctx, fnName, mysqlJSONSchemaRefSyntaxReason)
			}
			if index.nodes[item.pointer].baseExternal {
				return nil, moerr.NewNotSupportedf(ctx, "%s: %s", fnName, mysqlJSONSchemaExternalRefReason)
			}
			object["$ref"] = ref.target
			if target, ok := index.nodes[ref.target]; ok {
				index.nodes[item.pointer].edges = append(index.nodes[item.pointer].edges, ref.target)
				index.edgeVisits++
				index.refEdges++
				effectiveTargets[ref.target] = struct{}{}
				stack = append(stack, mysqlEffectiveSchemaPending{pointer: ref.target, value: target.value})
			}
			_ = stringValue
			// Draft 4 ignores siblings of $ref. The full-tree scan above has
			// already inspected them for external references and budgets.
			continue
		}
		for _, key := range []string{"allOf", "anyOf", "oneOf"} {
			if schemas, ok := object[key].([]any); ok && len(schemas) == 0 {
				return nil, moerr.NewInvalidArg(ctx, fnName, key+" must contain at least one schema")
			}
		}

		stack = append(stack, mysqlEffectiveSchemaChildren(item.pointer, object)...)
	}
	return effectiveTargets, nil
}

func mysqlEffectiveSchemaChildren(pointer string, object map[string]any) []mysqlEffectiveSchemaPending {
	children := make([]mysqlEffectiveSchemaPending, 0)
	pushNamed := func(key string) {
		value, ok := object[key].(map[string]any)
		if !ok {
			return
		}
		keys := make([]string, 0, len(value))
		for name := range value {
			keys = append(keys, name)
		}
		sort.Strings(keys)
		for _, name := range keys {
			children = append(children, mysqlEffectiveSchemaPending{
				pointer: mysqlJSONPointerPath(pointer, key, name), value: value[name],
			})
		}
	}
	for _, key := range []string{"properties", "patternProperties", "definitions"} {
		pushNamed(key)
	}
	if dependencies, ok := object["dependencies"].(map[string]any); ok {
		keys := make([]string, 0, len(dependencies))
		for key := range dependencies {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, ok := dependencies[key].(map[string]any); ok {
				children = append(children, mysqlEffectiveSchemaPending{
					pointer: mysqlJSONPointerPath(pointer, "dependencies", key), value: dependencies[key],
				})
			}
		}
	}
	for _, key := range []string{"additionalItems", "additionalProperties", "not"} {
		if _, ok := object[key].(map[string]any); ok {
			children = append(children, mysqlEffectiveSchemaPending{
				pointer: mysqlJSONPointerChild(pointer, key), value: object[key],
			})
		}
	}
	for _, key := range []string{"allOf", "anyOf", "oneOf"} {
		if schemas, ok := object[key].([]any); ok {
			for i, schema := range schemas {
				if _, ok := schema.(map[string]any); ok {
					children = append(children, mysqlEffectiveSchemaPending{
						pointer: mysqlJSONPointerPath(pointer, key, strconv.Itoa(i)), value: schema,
					})
				}
			}
		}
	}
	if schema, ok := object["items"].(map[string]any); ok {
		children = append(children, mysqlEffectiveSchemaPending{
			pointer: mysqlJSONPointerPath(pointer, "items"), value: schema,
		})
	} else if schemas, ok := object["items"].([]any); ok {
		for i, schema := range schemas {
			if _, ok := schema.(map[string]any); ok {
				children = append(children, mysqlEffectiveSchemaPending{
					pointer: mysqlJSONPointerPath(pointer, "items", strconv.Itoa(i)), value: schema,
				})
			}
		}
	}
	return children
}

func mysqlValidateSchemaRefGraph(ctx context.Context, index *mysqlJSONSchemaIndex) error {
	colors := make(map[string]uint8, len(index.nodes))
	memo := make(map[string]int, len(index.nodes))
	pointers := make([]string, 0, len(index.nodes))
	for pointer := range index.nodes {
		pointers = append(pointers, pointer)
	}
	sort.Strings(pointers)
	for _, start := range pointers {
		if colors[start] != 0 {
			continue
		}
		type frame struct {
			pointer string
			next    int
			longest int
		}
		stack := []frame{{pointer: start}}
		colors[start] = 1
		for len(stack) > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			last := len(stack) - 1
			current := &stack[last]
			edges := index.nodes[current.pointer].edges
			if current.next < len(edges) {
				target := edges[current.next]
				current.next++
				switch colors[target] {
				case 0:
					colors[target] = 1
					stack = append(stack, frame{pointer: target})
				case 1:
					return errMySQLJSONSchemaRefCycle
				case 2:
					if candidate := memo[target] + 1; candidate > current.longest {
						current.longest = candidate
					}
				}
				continue
			}
			colors[current.pointer] = 2
			memo[current.pointer] = current.longest
			stack = stack[:last]
			if len(stack) > 0 {
				parent := &stack[len(stack)-1]
				if candidate := current.longest + 1; candidate > parent.longest {
					parent.longest = candidate
				}
			}
		}
	}
	if memo["#"]+1 > mysqlJSONSchemaMaxDepth {
		return errMySQLJSONSchemaExpansion
	}
	return nil
}
