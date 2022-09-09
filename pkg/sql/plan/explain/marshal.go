// Copyright 2021 - 2022 Matrix Origin
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

package explain

type ExplainInfo struct {
	data     Data   `json:data`
	code     string `json:code`
	message  string `json:message`
	success  bool   `json:success`
	query_id string `json:queryid`
}

type Data struct {
	steps []Step `json:steps`
	uuid  string `json:uuid`
}

type Step struct {
	graphData                GraphData                `json:graphData`
	step                     int                      `json:step`
	description              string                   `json:description`
	timeInMs                 int                      `json:timeInMs`
	state                    string                   `json:state`
	stats                    Stats                    `json:stats`
	autoMaterializationStats AutoMaterializationStats `json:autoMaterializationStats`
}

type Stats struct {
}

type AutoMaterializationStats struct {
}

type GraphData struct {
	nodes  []Node  `json:nodes`
	edges  []Edge  `json:edges`
	labels []Label `json:labels`
	global Global  `json:global`
}

type Node struct {
	logicalId  int        `json:logicalId`
	name       string     `json:name`
	title      string     `json:title`
	labels     []Label    `json:labels`
	statistics Statistics `json:statistics`
	waits      []Wait     `json:waits`
	totalStats TotalStats `json:totalStats`
}

type Edge struct {
	id          int         `json:id`
	src         int         `json:src`
	dst         int         `json:dst`
	rows        int64       `json:rows`
	expressions interface{} `json:expressions`
}

type Label struct {
	name  string `json:name`
	value string `json:value`
}

type Wait struct {
	name       string  `json:name`
	value      float64 `json:value`
	percentage float64 `json:percentage`
}

type TotalStats struct {
	name       string  `json:name`
	value      float64 `json:value`
	percentage float64 `json:percentage`
}

type Global struct {
	statistics Statistics `json:statistics`
	waits      []Wait     `json:waits`
	totalStats TotalStats `json:totalStats`
}

type Statistics struct {
	IO      []StatisticValue `json:IO`
	Network []StatisticValue `json:Network`
	Pruning []StatisticValue `json:Pruning`
}

type StatisticValue struct {
	name  string `json:name`
	value int    `json:value`
	unit  string `json:unit`
}
