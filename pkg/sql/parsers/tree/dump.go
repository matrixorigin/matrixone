package tree

import "strconv"

type Dump struct {
	statementImpl
	Database    Identifier
	Tables      TableNames
	OutFile     string
	MaxFileSize int64
}

func (node *Dump) Format(ctx *FmtCtx) {
	ctx.WriteString("dump")
	if node.Database != "" {
		ctx.WriteString(" database ")
		ctx.WriteString(string(node.Database))
	}
	if node.Tables != nil {
		ctx.WriteString(" tables ")
		node.Tables.Format(ctx)
	}
	if node.OutFile != "" {
		ctx.WriteString(" into ")
		ctx.WriteString(node.OutFile)
	}
	if node.MaxFileSize != 0 {
		ctx.WriteString(" max_file_size ")
		ctx.WriteString(strconv.FormatInt(node.MaxFileSize, 10))
	}
}
