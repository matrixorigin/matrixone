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

func (vf *VersionFile) Destroy() error {
	if err := vf.Close(); err != nil {
		return err
	}
	name := vf.Name()
	err := os.Remove(name)
	return err
}
