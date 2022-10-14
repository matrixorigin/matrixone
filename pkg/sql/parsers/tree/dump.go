package tree

type Dump struct {
	statementImpl
	Database Identifier
	Table    *TableName
	All      bool
	OutFile  string
}

func (node *Dump) Format(ctx *FmtCtx) {
	ctx.WriteString("dump")
	if node.Database != "" {
		ctx.WriteString(" database ")
		ctx.WriteString(string(node.Database))
	}
	if node.Table != nil {
		ctx.WriteString(" table ")
		node.Table.Format(ctx)
	}
	if node.All {
		ctx.WriteString(" all")
	}
	if node.OutFile != "" {
		ctx.WriteString(" into ")
		ctx.WriteString(node.OutFile)
	}
}
