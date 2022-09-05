package objectio

type ColumnBlock struct {
	meta *ColumnMeta
}

func NewColumnBlock() *ColumnBlock {
	block := &ColumnBlock{}
	return block
}
