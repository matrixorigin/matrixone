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

package it

type BaseResources struct {
	Impl IResources
}

type BaseIterator struct {
	Err       error
	Result    interface{}
	Executor  ExecutorT
	Resources IResources
	Impl      Iterator
}

var (
	_ IResources = (*BaseResources)(nil)
	_ Iterator   = (*BaseIterator)(nil)
)

func NewBaseResources(impl IResources) IResources {
	res := &BaseResources{
		Impl: impl,
	}
	return res
}

func NewBaseIterator(impl Iterator, executor ExecutorT, resources IResources) Iterator {
	iter := &BaseIterator{
		Executor:  executor,
		Resources: resources,
		Impl:      impl,
	}
	return iter
}

func (res *BaseResources) IterResource(iter Iterator) {
	err := iter.PreIter()
	if err != nil {
		iter.SetErr(err)
		return
	}

	err = res.Impl.HandleResources(iter.Execute)
	if err != nil {
		iter.SetErr(err)
		return
	}

	err = iter.PreIter()
	iter.SetErr(err)
}

func (res *BaseResources) HandleResources(handle HandleT) error {
	return nil
}

func (iter *BaseIterator) SetErr(err error) {
	iter.Err = err
}

func (iter *BaseIterator) GetErr() error {
	return iter.Err
}

func (iter *BaseIterator) SetResult(r interface{}) {
	iter.Result = r
}

func (iter *BaseIterator) GetResult() interface{} {
	return iter.Result
}

func (iter *BaseIterator) PreIter() error {
	return nil
}

func (iter *BaseIterator) Iter() {
	iter.Resources.IterResource(iter)
}

func (iter *BaseIterator) PostIter() error {
	return nil
}

func (iter *BaseIterator) Execute(res interface{}) error {
	if iter.Executor != nil {
		return iter.Executor(res, iter.Impl)
	}
	return nil
}
