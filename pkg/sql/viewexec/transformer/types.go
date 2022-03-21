// Copyright 2021 Matrix Origin
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

package transformer

const (
	Sum = iota
	Avg
	Max
	Min
	Count
	StarCount
	ApproxCountDistinct
	Variance
)

var TransformerNames = [...]string{
	Sum:                 "sum",
	Avg:                 "avg",
	Max:                 "max",
	Min:                 "min",
	Count:               "count",
	StarCount:           "starcount",
	ApproxCountDistinct: "approx_count_distinct",
	// Variance:			 "var", // just sample implement of aggregate function for contributor.
}

var TransformerNamesMap map[string]int

type Transformer struct {
	Op    int
	Ref   int
	Name  string
	Alias string
}
