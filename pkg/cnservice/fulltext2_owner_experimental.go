//go:build fulltext2_base_file_reuse

package cnservice

import "github.com/matrixorigin/matrixone/pkg/fulltext2"

type fulltext2BaseReuseOwnerToken = fulltext2.BaseFileReuseOwnerToken

func initializeFulltext2BaseReuseOwner(service string) (*fulltext2BaseReuseOwnerToken, error) {
	return fulltext2.InitializeBaseFileReuseOwner(service)
}

func closeFulltext2BaseReuseOwner(service string, token *fulltext2BaseReuseOwnerToken) error {
	_ = service // retained for the default-build-compatible call signature
	return fulltext2.CloseBaseFileReuseOwner(token)
}
