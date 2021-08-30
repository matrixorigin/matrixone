package frontend

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func Test_block(t *testing.T) {
	dataLen := 200
	blk1 := NewBlock(0, dataLen)
	blk1.Reset()
	for i := 0; i < dataLen; i++ {
		blk1.Put(byte(i))
	}
	require.True(t, blk1.GetSpaceLen() == blk1.GetDataLen())
	require.True(t, blk1.GetReadIdx() == 0)
	require.True(t, blk1.GetWriteIdx() == blk1.GetSpaceLen())

	for i := 0; i < dataLen; i++ {
		o,v := blk1.Get()
		require.True(t, o)
		require.Equal(t, v, byte(i))
	}

	for i := 0; i < dataLen; i++ {
		o := blk1.UnGet(byte(i))
		require.True(t, o)
	}

	for i := 0; i < dataLen; i++ {
		o := blk1.UnPut()
		require.True(t, o)
	}
	require.True(t, blk1.IsEmpty())
	require.True(t, blk1.GetDataLen() == 0)
	require.False(t, blk1.IsFull())
	require.True(t, blk1.IsInRange(blk1.GetReadIdx()))
	require.True(t, blk1.IsInRange(blk1.GetWriteIdx()))
}

type pArgs struct {
	id int
	close *CloseFlag
	noDat *BlockPool
	hasDat *BlockPool
	wg *sync.WaitGroup
}

func producer(args *pArgs) {
	defer args.wg.Done()
	args.close.Open()
	for args.close.IsOpened() {
		b := args.noDat.GetBlock()
		time.Sleep(1 * time.Second)
		fmt.Printf("+++%d producer get a block %d \n",args.id,b.id)
		b.Reset()
		for i := 0; i < b.GetSpaceLen(); i++ {
			b.Put(byte(b.id))
		}
		time.Sleep(1 * time.Second)
		args.hasDat.PutBlock(b)
		fmt.Printf("+++%d producer put a block %d \n",args.id,b.id)
	}
	fmt.Printf("+++%d producer exit \n",args.id)
}

func consumer(args *pArgs) {
	defer args.wg.Done()
	args.close.Open()
	for args.close.IsOpened() {
		b := args.hasDat.GetBlock()
		time.Sleep(1 * time.Second)
		fmt.Printf("+++%d consumer get a block %d \n",args.id,b.id)
		for i := 0; i < b.GetSpaceLen(); i++ {
			o,v := b.Get()
			if !o {
				panic("read block failed.")
			}
			if int(v) != b.id {
				panic("data is wrong")
			}
		}
		time.Sleep(1 * time.Second)
		args.noDat.PutBlock(b)
		fmt.Printf("+++%d consumer put a block %d \n",args.id,b.id)
	}
	fmt.Printf("+++%d consumer exit \n",args.id)
}

func Test_blockpool(t *testing.T) {
	pcnt := 3
	ccnt := 5
	blockcnt := 10
	wg_p := &sync.WaitGroup{}
	wg_c := &sync.WaitGroup{}
	wg_p.Add(pcnt)
	wg_c.Add(ccnt)
	noData := NewBlockPool(blockcnt,10)
	hasData := NewEmptyBlockPool()
	p_args := make([]*pArgs,pcnt)
	c_args := make([]*pArgs,ccnt)
	for i := 0; i < pcnt; i++ {
		p_args[i] = &pArgs{
			id:    i,
			close: &CloseFlag{},
			noDat: noData,
			hasDat:  hasData,
			wg:    wg_p,
		}

		go producer(p_args[i])
	}

	for i := 0; i < ccnt; i++ {
		c_args[i] = &pArgs{
			id:    i,
			close: &CloseFlag{},
			noDat: noData,
			hasDat:  hasData,
			wg:    wg_c,
		}
		go consumer(c_args[i])
	}

	time.Sleep(5 * time.Second)
	for i := 0; i < pcnt; i++ {
		p_args[i].close.Close()
	}

	for i := 0; i < ccnt; i++ {
		c_args[i].close.Close()
	}

	wg_p.Wait()
	wg_c.Wait()

	noData.Release()
	hasData.Release()
	for i := 0; i < pcnt; i++ {
		require.True(t, p_args[i].close.IsClosed())
	}
	for i := 0; i < ccnt; i++ {
		require.True(t, c_args[i].close.IsClosed())
	}
	require.True(t, noData.close.IsClosed())
	require.True(t, hasData.close.IsClosed())
	require.True(t, len(noData.blockArray) + len(hasData.blockArray) == blockcnt)
}

