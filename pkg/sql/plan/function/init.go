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

package function

var AndFunctionEncodedID = encodeOverloadID(AND, 0)
var AndFunctionName = "and"
var EqualFunctionName = "="
var EqualFunctionEncodedID = encodeOverloadID(EQUAL, 0)
var SerialFunctionEncodeID = encodeOverloadID(SERIAL, 0)
var GroupConcatFunctionID = encodeOverloadID(GROUP_CONCAT, 0)
var AggSumOverloadID = encodeOverloadID(SUM, 0)

var InFunctionEncodedID = encodeOverloadID(IN, 0)
var InFunctionName = "in"
var PrefixInFunctionEncodedID = encodeOverloadID(PREFIX_IN, 0)
var PrefixInFunctionName = "prefix_in"
var PrefixEqualFunctionEncodedID = encodeOverloadID(PREFIX_EQ, 0)
var PrefixEqualFunctionName = "prefix_eq"
var L2DistanceFunctionEncodedID = encodeOverloadID(L2_DISTANCE, 0)

func init() {
	// init fixed type cast rule for binary operator like
	// +, -, x, /, div, >=, =, != and so on.
	initFixed1()
	initFixed2()

	// init implicit type cast rule.
	initFixed3()

	// init supported functions.
	initAllSupportedFunctions()
}
