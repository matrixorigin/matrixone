//go:build !fulltext2_base_file_reuse

package cnservice

type fulltext2BaseReuseOwnerToken struct{}

func initializeFulltext2BaseReuseOwner(_ string) (*fulltext2BaseReuseOwnerToken, error) {
	return nil, nil
}

func closeFulltext2BaseReuseOwner(_ string, _ *fulltext2BaseReuseOwnerToken) error { return nil }
