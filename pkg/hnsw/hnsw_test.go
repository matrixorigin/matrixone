package main

import (
	"fmt"
	"testing"

	usearch "github.com/unum-cloud/usearch/golang"
)

func TestUSearch(t *testing.T) {

	// Create Index
	vectorSize := 3
	vectorsCount := 100
	conf := usearch.DefaultConfig(uint(vectorSize))
	index, err := usearch.NewIndex(conf)
	if err != nil {
		panic("Failed to create Index")
	}
	defer index.Destroy()

	// Add to Index
	err = index.Reserve(uint(vectorsCount))
	for i := 0; i < vectorsCount; i++ {
		err = index.Add(usearch.Key(i), []float32{float32(i), float32(i + 1), float32(i + 2)})
		if err != nil {
			panic("Failed to add")
		}
	}

	// Search
	keys, distances, err := index.Search([]float32{0.0, 1.0, 2.0}, 3)
	if err != nil {
		panic("Failed to search")
	}
	fmt.Println(keys, distances)
}
