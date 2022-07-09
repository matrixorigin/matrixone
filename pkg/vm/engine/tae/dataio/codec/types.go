package codec

import (
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/tfs"
	"path/filepath"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

type Extension int16

const (
	ColumnBlockExt Extension = iota + 1000
	ComposedUpdatesExt
	ColumnUpdatesExt
	DeletesExt
	IndexExt
)

var ExtName map[Extension]string = map[Extension]string{
	ColumnBlockExt:     ".cblk",
	ComposedUpdatesExt: ".cus",
	ColumnUpdatesExt:   ".cu",
	DeletesExt:         ".del",
	IndexExt:           ".idx",
}

func ExtensionName(ext Extension) (name string) {
	name, found := ExtName[ext]
	if !found {
		panic(fmt.Sprintf("Unknown ext: %d", ext))
	}
	return
}

func EncodeDir(id *common.ID) (dir string) {
	return
}

func EncodeColBlkNameWithVersion(id *common.ID, version uint64, fs tfs.FS) (name string) {
	dir := EncodeDir(id)
	basename := fmt.Sprintf("%d-%d%s", id.Idx, version, ExtensionName(ColumnBlockExt))
	name = filepath.Join(dir, basename)
	return
}

func EncodeColBlkName(id *common.ID, fs tfs.FS) (name string) {
	dir := EncodeDir(id)
	basename := fmt.Sprintf("%d%s", id.Idx, ExtensionName(ColumnBlockExt))
	name = filepath.Join(dir, basename)
	return
}

// func EncodeComposedColumnFileName(id *common.ID, attrs []int, fs file.FS) (name string) {
// 	dir := fs.EncodeDir(id)
// 	name = fs.Join(dir, common.IntsToStr(attrs, "-"))
// 	return
// }
