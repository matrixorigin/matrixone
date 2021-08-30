package frontend

import (
	"fmt"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm/engine"
	"sync"
)

type LoadResult struct {
	Records, Deleted, Skipped, Warnings int
}

/*
It is not circle buffer
 */
type Block struct {
	data []byte

	id int

	/*
	readIdx <= writeIdx always holds
	 */
	
	//position where can be read
	readIdx int

	//position where can be put data into
	writeIdx int
}

func NewBlock(id int, l int) *Block {
	return &Block{
		data:     make([]byte,l),
		id:		id,
		readIdx:  0,
		writeIdx: 0,
	}
}

func (b *Block) Reset() {
	b.SetIdx(0,0)
}

func (b *Block) AssertLegal() {
	if !( b.readIdx <= b.writeIdx  &&
			b.IsInRange(b.readIdx) &&
			b.IsInRange(b.writeIdx) ) {
		//logutil.Errorf("readIdx %d > writeIdx %d ",b.readIdx,b.writeIdx)
		panic(fmt.Errorf("readIdx %d > writeIdx %d ",b.readIdx,b.writeIdx))
	}
}

func (b *Block) GetSpaceLen() int {
	return len(b.data)
}

func (b *Block) GetDataLen() int {
	b.AssertLegal()
	return b.writeIdx - b.readIdx
}

func (b *Block) GetData() []byte {
	return b.data
}

func (b *Block) SetIdx(r int,w int) {
	if r <= w  && b.IsInRange(r) && b.IsInRange(w) {
		b.readIdx = r
		b.writeIdx = w
	}
}

func (b *Block) GetReadIdx() int {
	return b.readIdx
}

func (b *Block) GetWriteIdx() int {
	return b.writeIdx
}

func (b *Block) IsEmpty() bool {
	return b.GetDataLen() <= 0
}

func (b *Block) IsFull() bool {
	return b.GetDataLen() == b.GetSpaceLen()
}

func (b *Block) IsInRange(idx int) bool {
	return idx >= 0 && idx <= b.GetSpaceLen()
}

/*
get the byte at the position readIdx
readIdx moves to the readIdx + 1
 */
func (b *Block) Get() (bool,byte) {
	if b.readIdx >= b.writeIdx {
		return false, 0
	}

	b.AssertLegal()
	d := b.data[b.readIdx]
	b.readIdx++
	b.AssertLegal()
	return true,d
}

/*
pick the byte at the position readIdx
readIdx does change
*/
func (b *Block) Pick() (bool,byte) {
	if b.readIdx >= b.writeIdx {
		return false, 0
	}

	b.AssertLegal()
	d := b.data[b.readIdx]
	return true,d
}

/*
unget the byte into the position readIdx
readIdx moves to the readIdx - 1
 */
func (b *Block) UnGet(d byte) bool {
	if b.readIdx <= 0 {
		return false
	}

	b.readIdx--
	b.AssertLegal()
	b.data[b.readIdx] = d
	return true
}

/*
put the byte at the position writeIdx
writeIdx moves to the writeIdx + 1
 */
func (b *Block) Put(d byte) bool {
	if b.writeIdx >= b.GetSpaceLen() {
		return false
	}

	b.AssertLegal()
	b.data[b.writeIdx] = d
	b.writeIdx++
	b.AssertLegal()
	return true
}

/*
unput the byte at the position writeIdx
writeIdx moves to the writeIdx - 1
*/
func (b *Block) UnPut() bool {
	if b.writeIdx <= b.readIdx {
		return false
	}

	b.writeIdx--
	b.AssertLegal()
	return true
}

type BlockPool struct {
	lock sync.Mutex
	cond *sync.Cond
	blockArray []*Block
	close *CloseFlag
}

func NewBlockPool(blockCnt,blockSize int) *BlockPool {
	bp := &BlockPool{
		blockArray: make([]*Block,blockCnt),
		close:      &CloseFlag{},
	}

	bp.cond = sync.NewCond(&bp.lock)
	for i := 0; i < blockCnt; i++ {
		bp.blockArray[i] = NewBlock(i,blockSize)
	}
	bp.close.Open()
	return bp
}

func NewEmptyBlockPool() *BlockPool {
	bp := &BlockPool{
		blockArray: nil,
		close:      &CloseFlag{},
	}

	bp.cond = sync.NewCond(&bp.lock)
	bp.close.Open()
	return bp
}

/*
unsafe in multi-thread environment
*/
func (bp *BlockPool) getUnusedBlock() *Block {
	if len(bp.blockArray) != 0 {
		return bp.blockArray[0]
	}
	return nil
}

/*
if there is no unused block, thread will be blocked.
 */
func (bp *BlockPool) GetBlock() *Block {
	var b *Block = nil
	bp.cond.L.Lock()
	for {
		if bp.close.IsClosed() {
			break
		}

		b = bp.getUnusedBlock()
		if b == nil {
			bp.cond.Wait()
		}else {
			break
		}
	}

	fmt.Printf("--- len %d\n",len(bp.blockArray))
	if b != nil {
		bp.blockArray = bp.blockArray[1:]
	}
	
	bp.cond.L.Unlock()
	bp.cond.Signal()
	return b
}

func (bp *BlockPool) PutBlock(b *Block) {
	bp.cond.L.Lock()
	bp.blockArray = append(bp.blockArray,b)
	bp.cond.L.Unlock()
	bp.cond.Signal()
}

func (bp *BlockPool) Release()  {
	bp.close.Close()
	bp.cond.Broadcast()
}

/*
LoadLoop reads data from stream, extracts the fields, and saves into the table
 */
func (mce *MysqlCmdExecutor) LoadLoop (load *tree.Load, dbHandler engine.Database,tableHandler engine.Relation) (*LoadResult,error){
	/*
	step1 : read block from file
	 */

	/*
	step2 : split into lines
	 */

	/*
	step3 : for every line, split into fields
	 */

	/*
	step4 : fields converted into column
	 */

	/*
	step5 : row of fields converted into column format
	 */

	/*
	step6 : append column to a batch
	 */

	/*
	step7 : write batch into the engine
	 */
	return nil, nil
}