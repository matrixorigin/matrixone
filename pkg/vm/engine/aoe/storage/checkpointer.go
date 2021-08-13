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

type checkpointerFactory struct {
	dir string
}

func NewCheckpointerFactory(dir string) *checkpointerFactory {
	factory := &checkpointerFactory{
		dir: dir,
	}
	return factory
}

func (f *checkpointerFactory) Create() *checkpointer {
	ck := &checkpointer{
		factory: f,
	}
	return ck
}

type checkpointer struct {
	factory *checkpointerFactory
	tmpfile string
}

func (ck *checkpointer) PreCommit(res md.Resource) error {
	if res == nil {
		log.Error("nil res")
		return errors.New("nil res")
	}
	var fname string
	switch res.GetResourceType() {
	case md.ResInfo:
		fname = MakeInfoCkpFileName(ck.factory.dir, res.GetFileName(), true)
	case md.ResTable:
		fname = MakeTableCkpFileName(ck.factory.dir, res.GetFileName(), res.GetTableId(), true)
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
	w, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer w.Close()
	err = res.Serialize(w)
	if err != nil {
		return err
	}
	ck.tmpfile = fname
	return nil
}

func (ck *checkpointer) Commit(res md.Resource) error {
	if len(ck.tmpfile) == 0 {
		return errors.New("Cannot Commit checkpoint, should do PreCommit before")
	}
	fname, err := FilenameFromTmpfile(ck.tmpfile)
	if err != nil {
		return err
	}
	// log.Infof("Commit CheckPoint: %s", fname)
	err = os.Rename(ck.tmpfile, fname)
	return err
}

func (ck *checkpointer) Load() error {
	// TODO
	return nil
}
