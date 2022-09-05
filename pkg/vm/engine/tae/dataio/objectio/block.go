package objectio

type Block struct {
	header  *BlockHeader
	columns []*ColumnBlock
}

func NewBlock() *Block {
	block := &Block{}
	return block
}
