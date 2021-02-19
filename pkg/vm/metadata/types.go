package metadata

import "matrixbase/pkg/container/types"

type Nodes []Node

type Node struct {
	Id   string `json:"id"`
	Addr string `json:"address"`
}

type Attribute struct {
	Alg  int     // compression algorithm
	Type types.T // type of attribute
	Name string  // name of attribute
}
