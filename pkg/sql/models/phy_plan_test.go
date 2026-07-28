// Copyright 2024 Matrix Origin
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

package models

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/resource"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	isFirstTrue = 1 << 0 // 0001 : isFirst = true
	isLastTrue  = 1 << 1 // 0010 : isLast = true

	isFirstFalse = 0 << 0 // 0000 : isFirst = false
	isLastFalse  = 0 << 1 // 0000 : isLast = false
)

var benchmarkExportPhyPlan *PhyPlan

func BenchmarkPhyPlanCloneForExport(b *testing.B) {
	for _, operatorCount := range []int{10, 100, 1000} {
		for _, withBackgroundQuery := range []bool{false, true} {
			name := fmt.Sprintf("operators=%d/background=%t", operatorCount, withBackgroundQuery)
			b.Run(name, func(b *testing.B) {
				plan := newBenchmarkPhyPlan(operatorCount, withBackgroundQuery)
				b.ReportAllocs()
				for b.Loop() {
					benchmarkExportPhyPlan = plan.CloneForExport()
				}
			})
		}
	}
}

func newBenchmarkPhyPlan(operatorCount int, withBackgroundQuery bool) *PhyPlan {
	var root *PhyOperator
	for i := 0; i < operatorCount; i++ {
		stats := &process.OperatorStats{
			CallNum:         i + 1,
			OperatorMetrics: map[process.MetricType]int64{process.OpScanTime: int64(i)},
			ExtraStats:      map[string]int64{"SpillBytes": int64(i)},
		}
		if withBackgroundQuery && i == 0 {
			stats.BackgroundQueries = []*planpb.Query{{
				Steps:      []int32{0},
				Headings:   []string{"background"},
				DetectSqls: []string{"select 1"},
			}}
		}
		operator := &PhyOperator{
			OpName:  "benchmark",
			NodeIdx: i,
			OpStats: stats,
		}
		if root != nil {
			operator.Children = []*PhyOperator{root}
		}
		root = operator
	}
	return &PhyPlan{LocalScope: []PhyScope{{RootOperator: root}}}
}

func newPhyPlanExportCloneFixture() (*PhyPlan, *process.OperatorStats) {
	stats := &process.OperatorStats{
		OperatorName:    "shared",
		CallNum:         7,
		OperatorMetrics: map[process.MetricType]int64{process.OpScanTime: 11},
		ExtraStats:      map[string]int64{"SpillBytes": 13},
		BackgroundQueries: []*planpb.Query{{
			Steps:      []int32{0},
			Headings:   []string{"background"},
			DetectSqls: []string{"select 1"},
			BackgroundQueries: []*planpb.Query{{
				Steps:    []int32{1},
				Headings: []string{"nested"},
			}},
		}},
	}
	sharedChild := &PhyOperator{
		OpName:  "child",
		NodeIdx: 1,
		OpStats: stats,
	}
	root := &PhyOperator{
		OpName:       "root",
		NodeIdx:      0,
		DestReceiver: []PhyReceiver{{Idx: 1, RemoteUuid: "receiver"}},
		OpStats:      stats,
		Children:     []*PhyOperator{sharedChild, sharedChild},
	}
	return &PhyPlan{
		Version:   "1.0",
		RetryTime: 3,
		Resource:  &resource.StatementResourceSummary{StatementWallNS: 17},
		LocalScope: []PhyScope{{
			Magic:      "Normal",
			Receiver:   []PhyReceiver{{Idx: 2, RemoteUuid: "local"}},
			DataSource: &PhySource{SchemaName: "db", RelationName: "tbl", Attributes: []string{"a", "b"}},
			PreScopes: []PhyScope{{
				Magic:        "Merge",
				RootOperator: sharedChild,
			}},
			RootOperator: root,
		}},
		RemoteScope: []PhyScope{{
			Magic:        "Remote",
			RootOperator: sharedChild,
		}},
	}, stats
}

func TestPhyPlanCloneForExportReferenceSchemaIsExplicit(t *testing.T) {
	source, stats := newPhyPlanExportCloneFixture()
	testCases := []struct {
		name                  string
		value                 any
		referenceBearingField []string
	}{
		{
			name:                  "PhyPlan",
			value:                 source,
			referenceBearingField: []string{"LocalScope", "RemoteScope", "Resource"},
		},
		{
			name:                  "PhyScope",
			value:                 source.LocalScope[0],
			referenceBearingField: []string{"Receiver", "DataSource", "PreScopes", "RootOperator"},
		},
		{
			name:                  "PhyReceiver",
			value:                 source.LocalScope[0].Receiver[0],
			referenceBearingField: nil,
		},
		{
			name:                  "PhySource",
			value:                 source.LocalScope[0].DataSource,
			referenceBearingField: []string{"Attributes"},
		},
		{
			name:                  "PhyOperator",
			value:                 source.LocalScope[0].RootOperator,
			referenceBearingField: []string{"DestReceiver", "OpStats", "Children"},
		},
		{
			name:                  "OperatorStats",
			value:                 stats,
			referenceBearingField: []string{"OperatorMetrics", "ExtraStats", "BackgroundQueries"},
		},
		{
			name:                  "StatementResourceSummary",
			value:                 source.Resource,
			referenceBearingField: nil,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			requireReferenceSchema(t, testCase.value, testCase.referenceBearingField)
		})
	}
}

func TestPhyPlanCloneForExportDetachesExecutionGraph(t *testing.T) {
	source, stats := newPhyPlanExportCloneFixture()

	got := source.CloneForExport()
	shallow := *source

	require.Equal(t, source, got)
	require.NotEmpty(t, findMutableAliases(source, &shallow),
		"positive control: a shallow plan copy must retain mutable aliases")
	require.Empty(t, findMutableAliases(source, got))
	require.NotSame(t, source, got)
	require.NotSame(t, source.Resource, got.Resource)
	require.NotSame(t, source.LocalScope[0].DataSource, got.LocalScope[0].DataSource)
	require.NotSame(t, source.LocalScope[0].RootOperator, got.LocalScope[0].RootOperator)
	require.NotSame(t, source.LocalScope[0].RootOperator.OpStats, got.LocalScope[0].RootOperator.OpStats)
	require.Same(t, got.LocalScope[0].RootOperator.Children[0], got.LocalScope[0].RootOperator.Children[1])
	require.Same(t, got.LocalScope[0].RootOperator.Children[0], got.LocalScope[0].PreScopes[0].RootOperator)
	require.Same(t, got.LocalScope[0].RootOperator.Children[0], got.RemoteScope[0].RootOperator)

	source.Resource.StatementWallNS = 19
	source.LocalScope[0].Receiver[0].Idx = 20
	source.LocalScope[0].DataSource.Attributes[0] = "mutated"
	source.LocalScope[0].RootOperator.DestReceiver[0].Idx = 21
	source.LocalScope[0].RootOperator.Children = nil
	stats.OperatorMetrics[process.OpScanTime] = 22
	stats.ExtraStats["SpillBytes"] = 23
	stats.Reset()

	require.Equal(t, uint64(17), got.Resource.StatementWallNS)
	require.Equal(t, 2, got.LocalScope[0].Receiver[0].Idx)
	require.Equal(t, "a", got.LocalScope[0].DataSource.Attributes[0])
	require.Equal(t, 1, got.LocalScope[0].RootOperator.DestReceiver[0].Idx)
	require.Len(t, got.LocalScope[0].RootOperator.Children, 2)
	require.Equal(t, 7, got.LocalScope[0].RootOperator.OpStats.CallNum)
	require.Equal(t, int64(11), got.LocalScope[0].RootOperator.OpStats.OperatorMetrics[process.OpScanTime])
	require.Equal(t, int64(13), got.LocalScope[0].RootOperator.OpStats.ExtraStats["SpillBytes"])

	var nilPlan *PhyPlan
	require.Nil(t, nilPlan.CloneForExport())
}

func requireReferenceSchema(t *testing.T, fixture any, expected []string) {
	t.Helper()
	value := reflect.ValueOf(fixture)
	require.True(t, value.IsValid())
	if value.Kind() == reflect.Pointer {
		require.False(t, value.IsNil())
		value = value.Elem()
	}
	require.Equal(t, reflect.Struct, value.Kind())

	typ := value.Type()
	actual := make([]string, 0)
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if containsMutableReference(field.Type) {
			actual = append(actual, field.Name)
		}
	}
	require.ElementsMatchf(t, expected, actual,
		"%s reference schema changed; classify every new field, update CloneForExport, and populate this fixture",
		typ)

	for _, fieldName := range expected {
		field := value.FieldByName(fieldName)
		require.Truef(t, field.IsValid(), "%s.%s is not present", typ, fieldName)
		require.Truef(t, hasPopulatedMutableReference(field),
			"%s.%s must be populated so the alias-detachment test exercises it", typ, fieldName)
	}
}

func containsMutableReference(typ reflect.Type) bool {
	switch typ.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map,
		reflect.Pointer, reflect.Slice, reflect.UnsafePointer:
		return true
	case reflect.Array:
		return containsMutableReference(typ.Elem())
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			if containsMutableReference(typ.Field(i).Type) {
				return true
			}
		}
	}
	return false
}

func hasPopulatedMutableReference(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Map, reflect.Slice:
		return !value.IsNil() && value.Len() > 0
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Pointer:
		return !value.IsNil()
	case reflect.UnsafePointer:
		return !value.IsNil()
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			if hasPopulatedMutableReference(value.Index(i)) {
				return true
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			if hasPopulatedMutableReference(value.Field(i)) {
				return true
			}
		}
	}
	return false
}

type mutableReferenceIdentity struct {
	kind    reflect.Kind
	typ     reflect.Type
	pointer uintptr
}

func findMutableAliases(source, clone any) []string {
	sourceIdentities := make(map[mutableReferenceIdentity]string)
	cloneIdentities := make(map[mutableReferenceIdentity]string)
	collectMutableReferenceIdentities(
		reflect.ValueOf(source), "source", sourceIdentities, make(map[mutableReferenceIdentity]struct{}))
	collectMutableReferenceIdentities(
		reflect.ValueOf(clone), "clone", cloneIdentities, make(map[mutableReferenceIdentity]struct{}))

	aliases := make([]string, 0)
	for identity, clonePath := range cloneIdentities {
		if sourcePath, ok := sourceIdentities[identity]; ok {
			aliases = append(aliases, fmt.Sprintf(
				"%s and %s share %s at %#x",
				sourcePath, clonePath, identity.typ, identity.pointer))
		}
	}
	sort.Strings(aliases)
	return aliases
}

func collectMutableReferenceIdentities(
	value reflect.Value,
	path string,
	identities map[mutableReferenceIdentity]string,
	visited map[mutableReferenceIdentity]struct{},
) {
	if !value.IsValid() {
		return
	}

	switch value.Kind() {
	case reflect.Interface:
		if !value.IsNil() {
			collectMutableReferenceIdentities(value.Elem(), path, identities, visited)
		}
	case reflect.Pointer:
		if value.IsNil() {
			return
		}
		identity := mutableReferenceIdentity{kind: value.Kind(), typ: value.Type(), pointer: value.Pointer()}
		identities[identity] = path
		if _, ok := visited[identity]; ok {
			return
		}
		visited[identity] = struct{}{}
		collectMutableReferenceIdentities(value.Elem(), path+"*", identities, visited)
	case reflect.Map:
		if value.IsNil() {
			return
		}
		identity := mutableReferenceIdentity{
			kind: value.Kind(), typ: value.Type(), pointer: uintptr(value.UnsafePointer()),
		}
		identities[identity] = path
		if _, ok := visited[identity]; ok {
			return
		}
		visited[identity] = struct{}{}
		iter := value.MapRange()
		for iter.Next() {
			collectMutableReferenceIdentities(iter.Key(), path+"[key]", identities, visited)
			collectMutableReferenceIdentities(iter.Value(), path+"[value]", identities, visited)
		}
	case reflect.Slice:
		if value.IsNil() {
			return
		}
		identity := mutableReferenceIdentity{kind: value.Kind(), typ: value.Type(), pointer: value.Pointer()}
		identities[identity] = path
		if _, ok := visited[identity]; ok {
			return
		}
		visited[identity] = struct{}{}
		for i := 0; i < value.Len(); i++ {
			collectMutableReferenceIdentities(value.Index(i), fmt.Sprintf("%s[%d]", path, i), identities, visited)
		}
	case reflect.Array:
		for i := 0; i < value.Len(); i++ {
			collectMutableReferenceIdentities(value.Index(i), fmt.Sprintf("%s[%d]", path, i), identities, visited)
		}
	case reflect.Struct:
		typ := value.Type()
		for i := 0; i < value.NumField(); i++ {
			collectMutableReferenceIdentities(
				value.Field(i), path+"."+typ.Field(i).Name, identities, visited)
		}
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		if !value.IsNil() {
			identity := mutableReferenceIdentity{kind: value.Kind(), typ: value.Type(), pointer: value.Pointer()}
			identities[identity] = path
		}
	}
}

func TestPhyPlanCloneForExportPreservesNilAndEmptyReferences(t *testing.T) {
	source := &PhyPlan{
		LocalScope:  []PhyScope{{RootOperator: &PhyOperator{}}},
		RemoteScope: []PhyScope{},
	}

	got := source.CloneForExport()

	require.Nil(t, got.Resource)
	require.Len(t, got.LocalScope, 1)
	require.Nil(t, got.LocalScope[0].Receiver)
	require.Nil(t, got.LocalScope[0].DataSource)
	require.Nil(t, got.LocalScope[0].PreScopes)
	require.NotNil(t, got.LocalScope[0].RootOperator)
	require.Nil(t, got.LocalScope[0].RootOperator.DestReceiver)
	require.Nil(t, got.LocalScope[0].RootOperator.OpStats)
	require.Nil(t, got.LocalScope[0].RootOperator.Children)
	require.NotNil(t, got.RemoteScope)
	require.Empty(t, got.RemoteScope)
}

func TestPhyPlanJSON(t *testing.T) {
	operatorStats := &process.OperatorStats{
		OperatorName:     "ExampleOperator",
		CallNum:          10,
		TimeConsumed:     5000,
		WaitTimeConsumed: 2000,
		MemorySize:       1024,
		SpillSize:        1024,
		InputRows:        1000,
		OutputRows:       950,
		InputSize:        2048,
		InputBlocks:      0,
		OutputSize:       1900,
		ScanBytes:        0,
		NetworkIO:        600,
		//TotalScanTime:         1500,
		//TotalInsertTime:       2500,
		//TotalServiceTime:      3500,
	}
	operatorStats.AddOpMetric(process.OpScanTime, 1500)
	operatorStats.AddOpMetric(process.OpInsertTime, 2500)
	operatorStats.AddOpMetric(process.OpIncrementTime, 3500)

	//----------------------------------------------------operator---------------------------------------------------

	phyOperator3_0 := PhyOperator{
		OpName:  "TableScan",
		NodeIdx: 0,
		Status:  isFirstTrue | isLastFalse,
		//IsFirst: true,
		//IsLast:  false,
		OpStats: operatorStats,
	}

	phyOperator3_1 := PhyOperator{
		OpName:  "Filter",
		NodeIdx: 0,
		Status:  isFirstFalse | isLastFalse,
		//IsFirst:  false,
		//IsLast:   false,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_0},
	}

	phyOperator3_2 := PhyOperator{
		OpName:  "Projection",
		NodeIdx: 0,
		Status:  isFirstFalse | isLastTrue,
		//IsFirst:  false,
		//IsLast:   true,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_1},
	}

	phyOperator3_3 := PhyOperator{
		OpName:  "Group",
		NodeIdx: 1,
		Status:  isFirstTrue | isLastFalse,
		//IsFirst:  true,
		//IsLast:   false,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_2},
	}

	phyOperator3_4 := PhyOperator{
		OpName:  "Connect",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		//IsFirst: false,
		//IsLast:  false,
		DestReceiver: []PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator3_3},
	}

	phyOperator2_0 := PhyOperator{
		OpName:  "Merge group",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastFalse,
		//IsFirst: false,
		//IsLast:  false,
		OpStats: operatorStats,
	}

	phyOperator2_1 := PhyOperator{
		OpName:  "Projection",
		NodeIdx: 1,
		Status:  isFirstFalse | isLastTrue,
		//IsFirst:  false,
		//IsLast:   true,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator2_0},
	}

	phyOperator2_2 := PhyOperator{
		OpName:  "projection",
		NodeIdx: 2,
		Status:  isFirstTrue | isLastFalse,
		//IsFirst:  true,
		//IsLast:   false,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator2_1},
	}

	phyOperator2_3 := PhyOperator{
		OpName:  "Connect",
		NodeIdx: 2,
		Status:  isFirstFalse | isLastFalse,
		//IsFirst: false,
		//IsLast:  false,
		OpStats: operatorStats,
		DestReceiver: []PhyReceiver{
			{
				Idx:        1,
				RemoteUuid: "",
			},
		},
		Children: []*PhyOperator{&phyOperator2_2},
	}

	phyOperator1_0 := PhyOperator{
		OpName:  "Merge",
		NodeIdx: 2,
		Status:  isFirstFalse | isLastTrue,
		//IsFirst: false,
		//IsLast:  true,
		OpStats: operatorStats,
	}

	phyOperator1_1 := PhyOperator{
		OpName:  "Output",
		NodeIdx: -1,
		Status:  isFirstFalse | isLastFalse,
		//IsFirst:  false,
		//IsLast:   false,
		OpStats:  operatorStats,
		Children: []*PhyOperator{&phyOperator1_0},
	}
	//---------------------------------------------------------scope---------------------------------------------------
	phyScope3 := PhyScope{
		Magic:        "Merge",
		PreScopes:    []PhyScope{},
		RootOperator: &phyOperator3_4,
		Receiver:     nil,
		DataSource:   &PhySource{SchemaName: "schema", RelationName: "table", Attributes: []string{"col1", "col2"}},
	}

	phyScope2 := PhyScope{
		Magic:        "Merge",
		PreScopes:    []PhyScope{phyScope3},
		RootOperator: &phyOperator2_3,
		Receiver: []PhyReceiver{
			{
				Idx:        0,
				RemoteUuid: "",
			},
		},
		DataSource: nil,
	}

	phyScope1 := PhyScope{
		Magic:        "Normal",
		PreScopes:    []PhyScope{phyScope2},
		RootOperator: &phyOperator1_1,
		Receiver: []PhyReceiver{
			{
				Idx:        1,
				RemoteUuid: "",
			},
		},
		DataSource: nil,
	}

	//------------------------------------------------------------------------------------------------------------------

	phyPlan := &PhyPlan{
		Version:     "1.0.0",
		LocalScope:  []PhyScope{phyScope1},
		RemoteScope: []PhyScope{phyScope1},
	}

	// Convert to JSON
	jsonStr, err := PhyPlanToJSON(phyPlan)
	if err != nil {
		fmt.Printf("Error serializing to JSON: %s", err)
		return
	}
	if !strings.Contains(jsonStr, `"MemorySize"`) {
		t.Fatalf("physical plan JSON lost operator memory diagnostic: %s", jsonStr)
	}
	fmt.Printf("JSON: %s\n", jsonStr)

	// Convert back from JSON
	phyPlanBack, err := JSONToPhyPlan(jsonStr)
	if err != nil {
		fmt.Printf("Error deserializing from JSON: %s", err)
		return
	}
	fmt.Printf("PhyPlan: %+v\n", phyPlanBack)

	//----------------------------------------------------
	jsonStr2, err := PhyPlanToJSON(&phyPlanBack)
	if err != nil {
		fmt.Printf("Error serializing to JSON: %s", err)
		return
	}
	fmt.Printf("JSON2: %s\n", jsonStr2)

	/*
		// Convert to JSON
		jsonData, err := json.MarshalIndent(phyPlan, "", "  ")
		if err != nil {
			log.Fatalf("Error serializing to JSON: %s", err)
		}

		// print JSON string
		fmt.Println(string(jsonData))
	*/
}
