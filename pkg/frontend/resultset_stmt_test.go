package frontend

import (
	"fmt"
	"testing"
)

type Done interface {
	a()
	b()
	c()
}

var _ Done = &base{}
var _ Done = &d1{}

type base struct {
}

func (b *base) a() {
	println("base a")
}

func (b *base) b() {
	println("base b")
}

func (b *base) c() {
	println("base c")
}

type d1 struct {
	*base
}

func (x *d1) b() {
	fmt.Println("d1 b")
}

type d2 struct {
	*base
}

func (x *d2) b() {
	fmt.Println("d2 b")
}

func do(d Done) {
	d.a()
	d.b()
	d.c()
}

func Test_polymorphic(t *testing.T) {
	i1 := &base{}
	i2 := &d1{}
	i3 := &d2{}

	do(i1)
	do(i2)
	do(i3)
}
