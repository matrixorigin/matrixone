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
	STANDARD_FUNCTION          FuncExplainLayout = iota // standard function
	UNARY_ARITHMETIC_OPERATOR                           // unary arithmetic operator
	BINARY_ARITHMETIC_OPERATOR                          // binary arithmetic operator
	UNARY_LOGICAL_OPERATOR                              // unary logical operator
	BINARY_LOGICAL_OPERATOR                             // binary logical operator
	COMPARISON_OPERATOR                                 // comparison operator
	CAST_EXPRESSION                                     // CAST expression
	CASE_WHEN_EXPRESSION                                // CASE ... WHEN ... expression
	BETWEEN_AND_EXPRESSION                              // BETWEEN ... AND ... expression
	IN_PREDICATE                                        // IN predicate
	IS_EXPRESSION                                       // IS expression
	IS_NOT_EXPRESSION                                   // IS NOT operator
	NOPARAMETER_FUNCTION                                // noparameter function
	DATE_INTERVAL_EXPRESSION                            // DATE expression, INTERVAL expression
	EXTRACT_FUNCTION                                    // EXTRACT function,such as EXTRACT(MONTH/DAY/HOUR/MINUTE/SECOND FROM p)
	POSITION_FUNCTION                                   // POSITION function, such as POSITION(substr IN str)
	EXISTS_ANY_PREDICATE
	UNKNOW_KIND_FUNCTION
)
