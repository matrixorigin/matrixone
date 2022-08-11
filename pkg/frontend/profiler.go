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

package frontend

/*
*
phase statistics
*/
type PhaseProfiler interface {
	/**
	start the statistics for the phase.
	name: the name of the phase
	*/
	StartPhase(name string)

	/**
	stop the statistics for the phase
	*/
	EndPhase()

	// ToString convert the phase info into the string
	ToString() string
}

// OperatorProfiler : operator statistics
type OperatorProfiler interface {
	//start the statistics for the operator
	StartOperator(operator interface{})

	//end the statistics for the operator
	EndOperator()

	//add the operator into the profiler
	AddOperator(operator interface{})

	//convert the operator info into the string
	ToString() string
}

// query statistics
type QueryProfiler interface {
	//start the statistics for the query
	StartQuery(string)

	//stop the statistics for the query
	EndQuery()

	//generate the statistics tree from the physical plan
	InitWithPlan()

	//add OperatorProfiler information into the query profiler
	AddOperatorProfiler(OperatorProfiler)

	//convert the profiler into the string
	ToString()
}
