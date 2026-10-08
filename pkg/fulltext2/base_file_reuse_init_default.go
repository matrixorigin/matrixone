//go:build !fulltext2_base_file_reuse

package fulltext2

const experimentalBaseFileReuseEnabled = false

// The normal build has no optional Base-file owner.  These no-op lifecycle
// entry points let CN service code keep one call site while preserving the
// production default loader and its ownership model.
type BaseFileReuseOwnerToken struct{}

func InitializeBaseFileReuseOwner(_ string) (*BaseFileReuseOwnerToken, error) { return nil, nil }

func CloseBaseFileReuseOwner(_ *BaseFileReuseOwnerToken) error { return nil }

// newExperimentalFulltext2Search deliberately falls back to the ordinary
// loader unless the private fulltext2_base_file_reuse build tag is enabled.
func newExperimentalFulltext2Search(cfg TableConfig, _ int64, _ int) *Fulltext2Search {
	return NewFulltext2Search(cfg)
}

// NewFulltext2SearchForExecution keeps the normal SQL path unchanged unless
// the private fulltext2_base_file_reuse experiment is explicitly built.
func NewFulltext2SearchForExecution(cfg TableConfig, _ string) *Fulltext2Search {
	return NewFulltext2Search(cfg)
}
