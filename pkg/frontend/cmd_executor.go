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

// CmdExecutor handle the command from the client
type CmdExecutor interface {
	// ExecRequest execute the request and get the response
	ExecRequest(req *Request) (*Response,error)

	Close()

	//the routine
	SetRoutine(*Routine)
}

type CmdExecutorImpl struct {
	CmdExecutor
	//sql parser
	//database engine

	routine *Routine
}

func (cei *CmdExecutorImpl) SetRoutine(r *Routine)  {
	cei.routine = r
}