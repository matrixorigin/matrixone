package it

import ()

type ExecutorT func(interface{}, Iterator) error
type HandleT func(interface{}) error

type IResources interface {
	IterResource(Iterator)
	HandleResources(HandleT) error
}

type Iterator interface {
	PreIter() error
	Iter()
	PostIter() error
	GetResult() interface{}
	GetErr() error
	SetResult(interface{})
	SetErr(error)
	Execute(interface{}) error
}
