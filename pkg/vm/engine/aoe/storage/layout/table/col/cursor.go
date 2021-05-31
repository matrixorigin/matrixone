package col

import (
	"errors"
	"io"
	nif "matrixone/pkg/vm/engine/aoe/storage/buffer/node/iface"
	// log "github.com/sirupsen/logrus"
)

type IScanCursor interface {
	io.Closer
	Next() bool
	Init() error
	IsInited() bool
}

type ScanCursor struct {
	CurrSeg IColumnSegment
	Current IColumnPart
	Handle  nif.IBufferHandle
	Inited  bool
}

func (c *ScanCursor) Next() bool {
	if c.Current == nil {
		return false
	}
	currBlkID := c.Current.GetID()
	c.Close()
	c.Current = c.Current.GetNext()
	if c.Current == nil {
		if c.CurrSeg == nil {
			return false
		}

		currBlk := c.CurrSeg.GetBlock(currBlkID)
		currBlk = currBlk.GetNext()
		if currBlk != nil {
			currBlk.InitScanCursor(c)
			return c.Current != nil
		}

		c.CurrSeg = c.CurrSeg.GetNext()
		if c.CurrSeg == nil {
			return false
		}
		c.CurrSeg.InitScanCursor(c)
	}
	return c.Current != nil
}

func (c *ScanCursor) IsInited() bool {
	return c.Inited
}

func (c *ScanCursor) Init() error {
	if c.Inited {
		// return errors.New("Cannot init already init'ed cursor")
		return nil
	}
	if c.Current == nil {
		return errors.New("Cannot init due to no block")
	}
	err := c.Current.InitScanCursor(c)
	if err != nil {
		return err
	}
	c.Inited = true
	return err
}

func (c *ScanCursor) Close() error {
	c.Inited = false
	if c.Handle != nil {
		err := c.Handle.Close()
		if err != nil {
			panic("logic error")
		}
		c.Handle = nil
		return nil
	}
	return nil
}
