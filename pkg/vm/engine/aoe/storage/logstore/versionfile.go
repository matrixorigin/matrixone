package logstore

import "os"

type VersionFile struct {
	*os.File
	Version uint64
	Size    int64
}
