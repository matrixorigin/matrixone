package engine

import (
	"errors"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"os"
	"path/filepath"

	log "github.com/sirupsen/logrus"
)

var (
	ErrAlreadyExist = errors.New("ckp already done")
)

type Checkpointer struct {
	Opts    *Options
	Dirname string
	TmpFile string
}

func NewCheckpointer(opts *Options, dirname string) *Checkpointer {
	ck := &Checkpointer{
		Opts:    opts,
		Dirname: dirname,
	}
	return ck
}

func (ck *Checkpointer) PreCommit(res md.Resource) error {
	if res == nil {
		log.Error("nil res")
		return errors.New("nil res")
	}
	var fname string
	switch res.GetResourceType() {
	case md.ResInfo:
		fname = MakeInfoCkpFileName(ck.Dirname, res.GetFileName(), true)
	case md.ResTable:
		fname = MakeTableCkpFileName(ck.Dirname, res.GetFileName(), res.GetTableId(), true)
	default:
		panic("not supported")
	}
	// log.Infof("PreCommit CheckPoint: %s", fname)
	if _, err := os.Stat(fname); err == nil {
		return ErrAlreadyExist
	}
	dir := filepath.Dir(fname)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		err = os.MkdirAll(dir, 0755)
		if err != nil {
			return err
		}
	}
	w, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer w.Close()
	err = res.Serialize(w)
	if err != nil {
		return err
	}
	ck.TmpFile = fname
	return nil
}

func (ck *Checkpointer) Commit(res md.Resource) error {
	if len(ck.TmpFile) == 0 {
		return errors.New("Cannot Commit checkpoint, should do PreCommit before")
	}
	fname, err := FilenameFromTmpfile(ck.TmpFile)
	if err != nil {
		return err
	}
	// log.Infof("Commit CheckPoint: %s", fname)
	err = os.Rename(ck.TmpFile, fname)
	return err
}

func (ck *Checkpointer) Load() error {
	// TODO
	return nil
}
