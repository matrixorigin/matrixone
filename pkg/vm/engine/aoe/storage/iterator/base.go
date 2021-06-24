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
