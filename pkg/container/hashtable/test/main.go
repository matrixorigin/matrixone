package main

import (
	"fmt"
	"matrixone/pkg/container/hashtable"
)

func main() {
	ht := &hashtable.MockStringHashTable{}
	ht.Init()
	key := []byte{48}
	//	ok, vp, err := ht.Insert(0, key)
	ok, vp, err := ht.Hs.Insert(0, hashtable.StringRef{Ptr: &key[0], Length: len(key)})
	fmt.Printf("%v, %v, %v\n", ok, vp, err)
}
