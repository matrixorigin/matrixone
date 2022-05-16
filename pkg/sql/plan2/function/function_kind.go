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
	STANDARD_FUNCTION          Kind = 0
	UNARY_ARITHMETIC_OPERATOR  Kind = 1
	BINARY_ARITHMETIC_OPERATOR Kind = 2
	UNARY_LOGICAL_OPERATOR     Kind = 3
	BINARY_LOGICAL_OPERATOR    Kind = 4
	COMPARISON_OPERATOR        Kind = 5
	CAST_EXPRESSION            Kind = 6
	CASE_WHEN_EXPRESSION       Kind = 7
	BETWEEN_AND_EXPRESSION     Kind = 8
	IN_EXISTS_EXPRESSION       Kind = 9
	IS_NULL_EXPRESSION         Kind = 10
	NOPARAMETER_FUNCTION       Kind = 11
	UNKNOW_KIND_FUNCTION       Kind = 12
)
