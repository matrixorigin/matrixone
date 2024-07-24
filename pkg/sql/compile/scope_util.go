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

package compile

// ----------------------------------------------------------------------------------------------------------------------
// Node expand mode when compiling plan scope
type NodeExpandType int

// type of scope
const (
	Merge1 NodeExpandType = 0
	Merge2 NodeExpandType = 1
)

// 1.Serial multi opertor linear, such as:
/*
-------@operatorA----->operatorB#---->
*/
const SerialMulOp NodeExpandType = 0

// 2.Serial single node linear, such as:
/*
--------@operatorA#------>
*/
const SerialSingleOp NodeExpandType = 1

// 3.ParallelMergeSingleComplete: NODE: MINUS, INTERSECT, INTERSECT_ALL
// NODE: MINUS, INTERSECT, INTERSECT_ALL
/*
---------\
           ----> @operator# ---->
---------/
*/
const ParallelMergeSingleComplete NodeExpandType = 2

// 4.Parallel left start merge end, such as:
// Join（Inner Join） ,LoopJoin ,Left ,LoopLeft ,Single ,LoopSingle ,Semi
// RightSemi ,LoopSemi ,Anti ,RightAnti ,LoopAnti ,Mark ,LoopMark ,Right
/*
---@leftOperator---\
	                ---> operator#
-------------------/
*/
const ParallelLeftStartMergeEnd NodeExpandType = 3
