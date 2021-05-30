package tree

import "bytes"

// PrintCtx contains formatted text of the node.
type PrintCtx struct {
	bytes.Buffer
}

// NodePrinter for formatted output of the node.
type NodePrinter interface {
	Print(ctx *PrintCtx)
}