package aoe

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var dir="../db"

func TestStorage(t *testing.T){
	_,err:=NewStorage(dir)
	assert.NoError(t,err,"NewStorage fail")
}