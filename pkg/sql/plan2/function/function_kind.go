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

type Kind int32

const (
	STANDARD_FUNCTION          Kind = 0 //standard function
	UNARY_ARITHMETIC_OPERATOR  Kind = 1 //unary arithmetic operator
	BINARY_ARITHMETIC_OPERATOR Kind = 2 //binary arithmetic operator
	UNARY_LOGICAL_OPERATOR     Kind = 3 // unary logical operator
	BINARY_LOGICAL_OPERATOR    Kind = 4 // binary logical operator
	COMPARISON_OPERATOR        Kind = 5 // comparison operator
	CAST_EXPRESSION            Kind = 6 // cast expression
	CASE_WHEN_EXPRESSION       Kind = 7 // case when expression
	BETWEEN_AND_EXPRESSION     Kind = 8
	IN_PREDICATE               Kind = 9  //query 'in' predicate
	EXISTS_ANY_PREDICATE       Kind = 10 //query predicate,such as exist,all,any
	IS_NULL_EXPRESSION         Kind = 11 // is null expression
	NOPARAMETER_FUNCTION       Kind = 12 // noparameter function
	DATE_INTERVAL_EXPRESSION   Kind = 13 // date expression,interval expression
	EXTRACT_FUNCTION           Kind = 14 // extract function,such as extract(MONTH/DAY/HOUR/MINUTE/SECOND FROM p)
	POSITION_FUNCTION          Kind = 15 // position function, such as POSITION(substr IN str)
	UNKNOW_KIND_FUNCTION       Kind = 16
)
