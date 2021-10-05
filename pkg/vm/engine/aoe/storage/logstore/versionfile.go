package logstore

import "os"

type VersionFile struct {
	*os.File
	Version uint64
	Size    int64
}

func (vf *VersionFile) Truncate(size int64) error {
	if err := vf.File.Truncate(size); err != nil {
		return err
	}
	vf.Size = size
	return nil
}
