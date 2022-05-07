package catalog

func ActiveWithNoTxnFilter(be *BaseEntry) bool {
	return !be.IsDroppedCommitted() && !be.InTxnOrRollbacked()
}

func AppendableBlkFilter(be *BlockEntry) bool {
	return be.IsAppendable()
}

func NonAppendableBlkFilter(be *BlockEntry) bool {
	return !be.IsAppendable()
}

type ComposedFilter struct {
	CommitFilters []func(*BaseEntry) bool
	BlockFilters  []func(*BlockEntry) bool
}

func NewComposedFilter() *ComposedFilter {
	return &ComposedFilter{
		CommitFilters: make([]func(*BaseEntry) bool, 0),
		BlockFilters:  make([]func(*BlockEntry) bool, 0),
	}
}

func (filter *ComposedFilter) AddCommitFilter(f func(*BaseEntry) bool) {
	filter.CommitFilters = append(filter.CommitFilters, f)
}

func (filter *ComposedFilter) AddBlockFilter(f func(*BlockEntry) bool) {
	filter.BlockFilters = append(filter.BlockFilters, f)
}

func (filter *ComposedFilter) FilteCommit(be *BaseEntry) bool {
	ret := false
	for _, f := range filter.CommitFilters {
		if !f(be) {
			ret = false
			break
		}
		ret = true
	}
	return ret
}

func (filter *ComposedFilter) FilteBlock(be *BlockEntry) bool {
	ret := false
	for _, f := range filter.BlockFilters {
		if !f(be) {
			ret = false
			break
		}
		ret = true
	}
	return ret
}
