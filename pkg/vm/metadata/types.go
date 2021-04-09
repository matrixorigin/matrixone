package metadata

import "matrixone/pkg/container/types"

type Nodes []Node

type Node struct {
	Id   string `json:"id"`
	Addr string `json:"address"`
}

type Attribute struct {
	Alg  int        // compression algorithm
	Name string     // name of attribute
	Type types.Type // type of attribute
}
