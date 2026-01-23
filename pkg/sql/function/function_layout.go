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

type FuncExplainLayout int32

const (
	STANDARD_FUNCTION FuncExplainLayout = iota
	UNARY_ARITHMETIC_OPERATOR
	BINARY_ARITHMETIC_OPERATOR
	UNARY_LOGICAL_OPERATOR
	BINARY_LOGICAL_OPERATOR
	MULTIARY_LOGICAL_OPERATOR
	COMPARISON_OPERATOR
	CAST_EXPRESSION
	CASE_WHEN_EXPRESSION
	BETWEEN_AND_EXPRESSION
	IN_PREDICATE
	IS_EXPRESSION
	IS_NOT_EXPRESSION
	NOPARAMETER_FUNCTION
	DATE_INTERVAL_EXPRESSION
	EXTRACT_FUNCTION  // EXTRACT function,such as EXTRACT(MONTH/DAY/HOUR/MINUTE/SECOND FROM p)
	POSITION_FUNCTION // POSITION function, such as POSITION(substr IN str)
	EXISTS_ANY_PREDICATE
	UNKNOW_KIND_FUNCTION
)
