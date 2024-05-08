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

package tree

import "github.com/matrixorigin/matrixone/pkg/common/reuse"

func init() {
	reuse.CreatePool[CreateExtension](
		func() *CreateExtension { return &CreateExtension{} },
		func(c *CreateExtension) { c.reset() },
		reuse.DefaultOptions[CreateExtension](), //.
	) //WithEnableChecker()

	reuse.CreatePool[LoadExtension](
		func() *LoadExtension { return &LoadExtension{} },
		func(l *LoadExtension) { l.reset() },
		reuse.DefaultOptions[LoadExtension](), //.
	) //WithEnableChecker()
}

type CreateExtension struct {
	statementImpl
	Language string
	Name     Identifier
	Filename Identifier
}

func NewCreateExtension(language string, name, filename Identifier) *CreateExtension {
	c := reuse.Alloc[CreateExtension](nil)
	c.Language = language
	c.Name = name
	c.Filename = filename
	return c
}

func (node *CreateExtension) Format(ctx *FmtCtx) {
	ctx.WriteString("create extension ")
	ctx.WriteString(node.Language)
	ctx.WriteString(" as ")
	node.Name.Format(ctx)
	ctx.WriteString(" file ")
	node.Filename.Format(ctx)
}

func (node *CreateExtension) Free() {
	reuse.Free[CreateExtension](node, nil)
}

func (node *CreateExtension) reset() {
	*node = CreateExtension{}
}

func (node CreateExtension) TypeName() string { return "tree.CreateExtension" }

type LoadExtension struct {
	statementImpl
	Name Identifier
}

func NewLoadExtension(name Identifier) *LoadExtension {
	c := reuse.Alloc[LoadExtension](nil)
	c.Name = name
	return c
}

func (node *LoadExtension) Format(ctx *FmtCtx) {
	ctx.WriteString("load ")
	node.Name.Format(ctx)
}

func (node *LoadExtension) Free() {
	reuse.Free[LoadExtension](node, nil)
}

func (node *LoadExtension) reset() {
	*node = LoadExtension{}
}

func (node LoadExtension) TypeName() string { return "tree.LoadExtension" }
