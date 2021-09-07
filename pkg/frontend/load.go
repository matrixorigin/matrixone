package frontend

import (
	"bytes"
	"fmt"
	"io"
	"matrixone/pkg/common/simdcsv"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/logutil"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm/engine"
	"os"
	"runtime/pprof"
	"strconv"
	"sync"
	"time"
)

type LoadResult struct {
	Records, Deleted, Skipped, Warnings uint64
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

	//block is in used
	inUse bool
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
	//b.AssertLegal()
	return b.writeIdx - b.readIdx
}

func (b *Block) GetData() []byte {
	return b.data
}

/*
get the slice [readIdx,writeIdx]
 */
func (b *Block) GetDataSlice() []byte{
	//b.AssertLegal()
	return b.data[b.readIdx:b.writeIdx]
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

	//b.AssertLegal()
	d := b.data[b.readIdx]
	b.readIdx++
	//b.AssertLegal()
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

func (b *Block) PutBytes(d []byte) bool {
	for _, v := range d {
		o := b.Put(v)
		if !o {
			return false
		}
	}
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
func (bp *BlockPool) getUnusedBlockUnsafe() *Block {
	if len(bp.blockArray) != 0 {
		return bp.blockArray[0]
	}
	return nil
}

/*
if there is no unused block, thread will be blocked.
return null, when the pool was closed.
 */
func (bp *BlockPool) GetBlock() *Block {
	var b *Block = nil
	bp.cond.L.Lock()
	for {
		if bp.close.IsClosed() {
			break
		}

		b = bp.getUnusedBlockUnsafe()
		if b == nil {
			bp.cond.Wait()
		}else {
			break
		}
	}

	//fmt.Printf("--- len %d\n",len(bp.blockArray))
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

func (bp *BlockPool) GetBlockCount() int {
	bp.cond.L.Lock()
	cnt := len(bp.blockArray)
	bp.cond.L.Unlock()
	return cnt
}

func (bp *BlockPool) Release()  {
	bp.close.Close()
	bp.cond.Broadcast()
}

/*
load data from file
 */
func loadDataFromFileRoutine(close *CloseFlag,dataFile *os.File,pool *BlockPool,outChan chan *Block) error {
	close.Open()
	var reads int
	var err error
	wait_pool := time.Duration(0)
	read_block := time.Duration(0)
	defer func() {
		fmt.Printf("wait pool %s read block %s \n",wait_pool.String(),read_block.String())
	}()

	x := 0
	for close.IsOpened() {
		x++
		fmt.Printf("wait a block %d\n",x)
		wait := time.Now()
		blk := pool.GetBlock()
		if blk == nil {
			continue
		}

		wait_pool += time.Since(wait)
		fmt.Printf("get a block read data %d\n",x)

		read := time.Now()
		reads,err = dataFile.Read(blk.GetData())
		read_block += time.Since(read)
		if reads == 0 && err == io.EOF{
			//end of file
			//write a nil to notice that reading data is over
			fmt.Printf("write block into channel 3 %d\n",x)
			outChan <- nil
			return nil
		}else if err != nil {
			fmt.Printf("write block into channel 2 %d\n",x)
			outChan <- nil
			return err
		}
		blk.SetIdx(0,reads)
		fmt.Printf("write block into channel 1 %d\n",x)
		//output data block into channel
		outChan <- blk
	}
	return nil
}

type LINE_STATE int
const (
	//in parsing prefix
	LINE_STATE_PROCESS_PREFIX LINE_STATE = iota
	//in skip unused byte
	LINE_STATE_SKIP_UNUSED_BYTES
	//in parsing field
	LINE_STATE_PROCESS_FIELD_FIRST_BYTE
	//in parsing field with "ENCLOSED BY"
	LINE_STATE_PROCESS_FIELD_ENCLOSED
	//in parsing field without "ENCLOSED BY"
	LINE_STATE_PROCESS_FIELD_WITHOUT_ENCLOSED
	//in parsing field skip bytes
	LINE_STATE_PROCESS_FIELD_SKIP_BYTES
	//parsing line has done
	LINE_STATE_DONE
)

type ParseLineHandler struct {
	status LINE_STATE

	//block has been processed partially
	partialBlocks []*Block

	//blocks has been used. for recycling them
	popedBlocks []*Block

	//load reference
	load *tree.Load
	fields *tree.Fields
	lines *tree.Lines

	//parsed lines, fields, partial field, skipped bytes
	lineArray [][][]byte
	capacityOfLineAarray int
	//index of line in line array
	lineIdx int
	colIdx int

	fieldsArray [][]byte
	fieldData []byte
	skipBytes []byte
	lineCount uint64

	//batch
	batchSize int
	batchData *batch.Batch
	batchFilled int
	//map column id in from data to column id in table
	dataColumnId2TableColumnId []int
	timestamp uint64

	//simd csv
	simdCsvLineArray [][]string
	simdCsvReader *simdcsv.Reader
	simdCsvLineOutChan chan simdcsv.LineOut
	simdCsvLineOutRoutineClose *CloseFlag

	//storage
	dbHandler engine.Database
	tableHandler engine.Relation

	//result of load
	result *LoadResult

	//for debug
	packLine func([][][]byte)

	row2col time.Duration
	fillBlank  time.Duration
	toStorage time.Duration

	writeBatch time.Duration
	resetBatch time.Duration

	prefix time.Duration
	skip_bytes time.Duration


	process_field time.Duration
	split_field time.Duration
	split_before_loop time.Duration
	wait_loop time.Duration
	handler_get time.Duration
	wait_switch time.Duration
	field_first_byte time.Duration
	field_enclosed time.Duration
	field_without time.Duration
	field_skip_bytes time.Duration

	callback  time.Duration
	asyncChan time.Duration
	csvLineArray1 time.Duration
	csvLineArray2 time.Duration
	asyncChanLoop time.Duration
}

func (plh *ParseLineHandler) Status() LINE_STATE {
	return plh.status
}

func (plh *ParseLineHandler) Fields() *tree.Fields {
	return plh.fields
}

func (plh *ParseLineHandler) Lines() *tree.Lines {
	return plh.lines
}

func (plh *ParseLineHandler) ClearLineArray() {
	if len(plh.lineArray) != 0 {
		plh.lineArray = plh.lineArray[:0]
	}
}

func (plh *ParseLineHandler) CountOfLines () int {
	return plh.lineIdx
}

//drop ignored lines
func (plh *ParseLineHandler)IgnoreLine() bool {
	return plh.lineCount < plh.load.IgnoredLines
}

func (plh *ParseLineHandler) PrintLastLine(pre string)  {
	fmt.Printf("%s",pre)
	for i := 0; i < plh.colIdx; i++ {
		fmt.Printf("[%d %v] ",i,string(plh.lineArray[plh.lineIdx][i]))
	}
	fmt.Printf("\n")
}

func (plh *ParseLineHandler) PrintLineArray(pre string)  {
	fmt.Printf("len %d %s\n",plh.CountOfLines(),pre)
	for i := 0; i < plh.lineIdx; i++ {
		for j := 0; j < len(plh.lineArray[i]); j++ {
			fmt.Printf("[%d %d %v] ",i,j,string(plh.lineArray[i][j]))
		}
		fmt.Printf("\n")
	}

	plh.PrintLastLine("")
}

func (plh *ParseLineHandler) AppendField(field []byte) {
	if plh.IgnoreLine() {
		return
	}
	//fmt.Printf("-[%v][%s] \n",field,string(field))
	if plh.lineIdx >= len(plh.lineArray) {
		plh.lineArray = append(plh.lineArray,nil)
	}

	if plh.colIdx >= len(plh.lineArray[plh.lineIdx]) {
		dst := make([]byte,len(field))
		dst = append(dst[:0],field...)
		plh.lineArray[plh.lineIdx] = append(plh.lineArray[plh.lineIdx],dst)
		plh.colIdx = len(plh.lineArray[plh.lineIdx])
	}else{
		dst := plh.lineArray[plh.lineIdx][plh.colIdx]
		if field == nil {
			dst = dst[:0]
		}else {
			dst = dst[:0]
			dst = append(dst,field...)
		}
		plh.lineArray[plh.lineIdx][plh.colIdx] = dst
		plh.colIdx++
	}
	//fmt.Printf("+[%v][%s] \n",
	//	plh.lineArray[plh.lineIdx][plh.colIdx-1],
	//	string(plh.lineArray[plh.lineIdx][plh.colIdx-1]))
	//plh.PrintLineArray("===>")
}

func (plh *ParseLineHandler) ResetLineArray(){
	plh.lineIdx=0
	plh.colIdx=0
	//for i := 0; i < plh.capacityOfLineAarray; i++ {
	//	plh.lineArray = plh.lineArray[:0]
	//}
}

func (plh *ParseLineHandler) Newline() error{
	if plh.IgnoreLine() {
		plh.lineCount++
		return nil
	}

	plh.lineArray[plh.lineIdx] = plh.lineArray[plh.lineIdx][:plh.colIdx]

	plh.lineCount++
	plh.lineIdx++
	plh.colIdx=0

	//line array is full, save it to the storage
	if plh.CountOfLines() == plh.capacityOfLineAarray {
		//fmt.Printf("-----newline linecount %d lineidx %d cap %d\n",
		//	plh.lineCount,plh.lineIdx,plh.capacityOfLineAarray)
		//fmt.Printf("-----\n")
		err := saveParsedLinesToBatchImprove(plh, false)
		if err != nil{
			return err
		}
		plh.ResetLineArray()
	}

	//plh.PrintLineArray(">>>>")
	return nil
}

/*
read next byte.
readidx changes in the block.
 */
func (plh *ParseLineHandler) Get() (bool,byte) {
	//wait_a := time.Now()
	//defer func() {
	//	plh.handler_get += time.Since(wait_a)
	//}()
	popCnt := 0
	for _, pBlk := range plh.partialBlocks {
		if pBlk.GetDataLen() == 0{
			popCnt++
		}else {
			break
		}
	}
	for i := 0; i < popCnt; i++ {
		plh.PopBlock()
	}
	if len(plh.partialBlocks) == 0 {
		return false, 0
	}
	return plh.partialBlocks[0].Get()
}

func (plh *ParseLineHandler) GetPartialBlocks() []*Block {
	return plh.partialBlocks
}

func (plh *ParseLineHandler) GetPopedBlocks() []*Block {
	return plh.popedBlocks
}

func (plh *ParseLineHandler) ClearPopedBlocks() {
	plh.popedBlocks = plh.popedBlocks[:0]
}

func (plh *ParseLineHandler) GetPartialBlocksCount () int {
	return len(plh.partialBlocks)
}

func (plh *ParseLineHandler) GetDataLenInPartialBlocks() int {
	partDataLen := 0
	if plh.partialBlocks != nil {
		for _, pb := range plh.partialBlocks {
			partDataLen += pb.GetDataLen()
		}
	}
	return partDataLen
}

func (plh *ParseLineHandler) PopBlock() *Block {
	blk := plh.partialBlocks[0]
	plh.partialBlocks = plh.partialBlocks[1:]
	//put it into poped queue
	plh.popedBlocks = append(plh.popedBlocks, blk)
	return blk
}

func (plh *ParseLineHandler) PushBlock(blk *Block)  {
	plh.partialBlocks = append(plh.partialBlocks, blk)
}

func (plh *ParseLineHandler) SwitchStatusTo(st LINE_STATE) {
	if st != plh.status {
		//old := plh.status
		plh.status = st
		//logutil.Infof("switch parsing line status from %d to %d.",old,st)
	}
}

func (plh *ParseLineHandler) Callback (lines [][]string, line []string) error {
	wait_a := time.Now()
	defer func() {
		plh.callback += time.Since(wait_a)
	}()
	if line != nil {
		//step 1 : skip dropped lines
		if plh.lineCount < plh.load.IgnoredLines {
			plh.lineCount++
			return nil
		}

		plh.lineCount++

		//step 2 : append line into line array
		plh.simdCsvLineArray[plh.lineIdx] = line
		plh.lineIdx++

		if plh.lineIdx == plh.batchSize {
			//step 3 : save into storage
			err := saveParsedLinesToBatchSimdCsv(plh,false)
			if err != nil {
				return err
			}

			plh.lineIdx = 0
		}

	}else if lines != nil {
		from := 0
		countOfLines := len(lines)
		//step 1 : skip dropped lines
		if plh.lineCount < plh.load.IgnoredLines {
			skipped := MinUint64(uint64(countOfLines), plh.load.IgnoredLines - plh.lineCount)
			plh.lineCount += skipped
			from += int(skipped)
		}

		fill := 0
		//step 2 : append lines into line array
		for i := from; i < countOfLines;  i += fill {
			fill = Min(countOfLines - i, plh.batchSize - plh.lineIdx)
			for j := 0; j < fill; j++ {
				plh.simdCsvLineArray[plh.lineIdx] = lines[i + j]
				plh.lineIdx++
			}

			if plh.lineIdx == plh.batchSize {
				//step 3 : save into storage
				err := saveParsedLinesToBatchSimdCsv(plh,false)
				if err != nil {
					return err
				}

				plh.lineIdx = 0
			}
		}
	}
	return nil
}

func (plh *ParseLineHandler) getLineOutFromSimdCsvRoutine(wg *sync.WaitGroup) error{
	plh.simdCsvLineOutRoutineClose.Open()
	defer wg.Done()
	wait_a := time.Now()
	defer func() {
		plh.asyncChan += time.Since(wait_a)
	}()
	for lineOut :=range plh.simdCsvLineOutChan {
		wait_d := time.Now()
		if lineOut.Line == nil  && lineOut.Lines == nil {
			break
		}
		if lineOut.Line != nil {
			//step 1 : skip dropped lines
			if plh.lineCount < plh.load.IgnoredLines {
				plh.lineCount++
				return nil
			}

			wait_b := time.Now()
			//step 2 : append line into line array
			plh.simdCsvLineArray[plh.lineIdx] = lineOut.Line
			plh.lineIdx++

			plh.csvLineArray1 += time.Since(wait_b)

			if plh.lineIdx == plh.batchSize {
				//step 3 : save into storage
				err := saveParsedLinesToBatchSimdCsv(plh,false)
				if err != nil {
					return err
				}

				plh.lineIdx = 0
			}

		}else if lineOut.Lines != nil {
			from := 0
			countOfLines := len(lineOut.Lines)
			//step 1 : skip dropped lines
			if plh.lineCount < plh.load.IgnoredLines {
				skipped := MinUint64(uint64(countOfLines), plh.load.IgnoredLines - plh.lineCount)
				plh.lineCount += skipped
				from += int(skipped)
			}

			fill := 0
			//step 2 : append lines into line array
			for i := from; i < countOfLines;  i += fill {
				fill = Min(countOfLines - i, plh.batchSize - plh.lineIdx)
				wait_c := time.Now()
				for j := 0; j < fill; j++ {
					plh.simdCsvLineArray[plh.lineIdx] = lineOut.Lines[i + j]
					plh.lineIdx++
				}
				plh.csvLineArray2 += time.Since(wait_c)

				if plh.lineIdx == plh.batchSize {
					//step 3 : save into storage
					err := saveParsedLinesToBatchSimdCsv(plh,false)
					if err != nil {
						return err
					}

					plh.lineIdx = 0
				}
			}
		}
		plh.asyncChanLoop += time.Since(wait_d)
	}
	return nil
}
/*
find substring among multiple blocks
 */
func findStringAmongBlocks(handler *ParseLineHandler,blk *Block,sub string,nextStatus LINE_STATE){
	lenOfSub := len(sub)
	if lenOfSub == 0 {
		handler.PushBlock(blk)
		/*
			substring is empty string "".
			skip nothing
		*/
		handler.SwitchStatusTo(nextStatus)
	}else{
		//skip "substring" and all bytes before it and including it
		//step 1 : find "substring" in blocks
		//step 2 : skip bytes before "substring" and "substring"

		if handler.GetPartialBlocksCount() == 0 {
			//branch 1 : there is no partial block.
			if blk.GetDataLen() < lenOfSub {
				handler.PushBlock(blk)
			}else{
				ret := bytes.Index(blk.GetDataSlice(),[]byte(sub))
				if  ret == -1 {
					//does not find "substring"
					//preserve len("substring") -1 bytes at end
					//skip bytes before the position
					//state does not change.
					wIdx := blk.GetWriteIdx()
					newRIdx := wIdx - lenOfSub + 1
					blk.SetIdx(newRIdx,wIdx)
					handler.PushBlock(blk)
				} else {
					//find "substring"
					//state changes
					newRIdx := ret + lenOfSub
					blk.SetIdx(newRIdx,blk.GetWriteIdx())
					if blk.GetDataLen() > 0{
						handler.PushBlock(blk)
					}
					handler.SwitchStatusTo(nextStatus)
				}
			}
		}else{
			handler.PushBlock(blk)
			//branch 2 : there is partial block.
			partDataLen := handler.GetDataLenInPartialBlocks()

			//there is not enough bytes, even a new block will be put in.
			if partDataLen >= lenOfSub {
				//find "substring" in many blocks
				partBlocks := handler.GetPartialBlocks()
				/*
					Currently, partialBlocks joins together and copied into a new allocated buffer.
					TODO:refine performance in the future
				*/
				buffer := make([]byte,0,partDataLen)
				pos := 0
				for _, pBlk := range partBlocks {
					buffer = append(buffer,pBlk.GetDataSlice()...)
				}

				ret := bytes.Index(buffer,[]byte(sub))
				newRIdx := 0
				if ret == -1 {
					//does not find "substring"
					newRIdx = partDataLen - lenOfSub + 1
				}else{
					//find "substring"
					//state change
					newRIdx = ret + lenOfSub
					handler.SwitchStatusTo(nextStatus)
				}

				pos = 0
				popCnt := 0
				for _,pBlk := range partBlocks{
					if newRIdx >= pos && newRIdx < pos + pBlk.GetDataLen() {
						//find block where newRIdx is in, split it
						break
					}
					pos += pBlk.GetDataLen()
					popCnt++
				}
				//pop blk
				for i := 0; i < popCnt; i++ {
					handler.PopBlock()
				}

				//!!!!partBlocks changed
				partBlocks = handler.GetPartialBlocks()
				if len(partBlocks) != 0{
					pBlk := partBlocks[0]
					pBlk.SetIdx(newRIdx - pos,pBlk.GetWriteIdx())
				}
			}
		}
	}
}

const (
	NO_FOUND = 0
	FIELD_END = 1
	LINE_END = 2
)

/*
find substring bytes at the end of another bytes.
 */
func findSubBytesAtEnd(s []byte,sub string) bool {
	sLen := len(s)
	subLen := len(sub)
	if subLen > sLen{
		return false
	}

	offset := sLen - subLen
	for i := subLen - 1; i >= 0 ; i-- {
		if sub[i] != s[ offset + i] {
			return false
		}
	}

	return true
}

/*
TODO:refine performance
find FIELD TERMINATED BY : return FIELD_END
find LINE TERMINATED BY : return LINE_END
change state if finding FIELD or LINE TERMINATED BY,else not
 */
func handleTerminatedBy(handler *ParseLineHandler,fieldData []byte,fieldTerminatedBy,lineTerminatedBy string) (int,[]byte){
	lenOfFieldTerminated := len(fieldTerminatedBy)
	lenOfLineTerminated := len(lineTerminatedBy)
	fieldLen := len(fieldData)
	found := NO_FOUND
	if fieldLen >= lenOfFieldTerminated {//check FIELDS "TERMINATED BY"
		if findSubBytesAtEnd(fieldData,fieldTerminatedBy) {//find "TERMINATED BY"
			//remove "TERMINATED BY" part
			//state changes
			fieldData = fieldData[:fieldLen - lenOfFieldTerminated]
			handler.SwitchStatusTo(LINE_STATE_PROCESS_FIELD_FIRST_BYTE)
			return FIELD_END, fieldData
		}
	}
	if fieldLen >= lenOfLineTerminated {//check FIELDS "TERMINATED BY"
		if findSubBytesAtEnd(fieldData,lineTerminatedBy) {//find "TERMINATED BY"
			//remove "TERMINATED BY" part
			//state changes
			fieldData = fieldData[:fieldLen - lenOfLineTerminated]
			handler.SwitchStatusTo(LINE_STATE_PROCESS_FIELD_FIRST_BYTE)
			return LINE_END, fieldData
		}
	}
	return found,fieldData
}

/*
split blocks into fields
 */
func splitFields(handler *ParseLineHandler,blk *Block)([][][]byte,[][]byte,[]byte) {
	wait_split := time.Now()
	defer func() {
		handler.split_field += time.Since(wait_split)
	}()
	if !(handler.Status() == LINE_STATE_PROCESS_FIELD_FIRST_BYTE ||
		handler.Status() == LINE_STATE_PROCESS_FIELD_ENCLOSED ||
		handler.Status() == LINE_STATE_PROCESS_FIELD_WITHOUT_ENCLOSED ||
		handler.Status() == LINE_STATE_PROCESS_FIELD_SKIP_BYTES) {
		return nil,nil,nil
	}
	fields := handler.fields
	lines := handler.lines

	if blk != nil {
		handler.PushBlock(blk)
	}

	dataLen := handler.GetDataLenInPartialBlocks()
	if dataLen == 0{
		return nil,nil,nil
	}

	handler.split_before_loop += time.Since(wait_split)

	wait_loop := time.Now()
	/*
	reading bytes across blocks
	step 1 : read a byte
	step 2 : if the byte == "ENCLOSED BY", then anything behind it and before next
		"ENCLOSED BY" is a field.
	step 3 : if the byte != "ENCLOSED BY", then anything includes it ,behind it and before
		next "ENCLOSED BY" is a field.
	 */
	lineArray := handler.lineArray
	fieldsArray := handler.fieldsArray
	fieldData := handler.fieldData
	skipBytes := handler.skipBytes
	for  {
		ok,d := handler.Get()
		if !ok {
			break
		}

		wait_switch := time.Now()
		switch handler.Status() {
		case LINE_STATE_PROCESS_FIELD_FIRST_BYTE://first byte
			//if fieldData != nil {
			//	//append fieldData into fieldsArray
			//	fieldsArray = append(fieldsArray,fieldData)
			//	fieldData = nil
			//}
			wait_a := time.Now()
			if d == fields.EnclosedBy {//first byte == "ENCLOSED BY"
				//drop first byte
				handler.SwitchStatusTo(LINE_STATE_PROCESS_FIELD_ENCLOSED)
			}else{
				//d is the first byte of field
				fieldData = append(fieldData,d)
				found := NO_FOUND
				found,fieldData = handleTerminatedBy(handler,fieldData,fields.Terminated,lines.TerminatedBy)
				if found == NO_FOUND {//does not find FIELD and LINE "TERMINATED BY"
					handler.SwitchStatusTo(LINE_STATE_PROCESS_FIELD_WITHOUT_ENCLOSED)
				}else if found == FIELD_END {
					//if len(fieldData) != 0, then a field with bytes
					//if len(fieldData) == 0, then a empty string field like ",,"
					//if len(fieldData) != 0 {
						//append fieldData into fieldsArray
						fieldsArray = append(fieldsArray,fieldData)
						fieldData = nil
					//}
				}else if found == LINE_END {
					if len(fieldData) != 0 {
						//append fieldData into fieldsArray
						fieldsArray = append(fieldsArray,fieldData)
						fieldData = nil
					}
					if len(fieldsArray) != 0 {
						lineArray = append(lineArray,fieldsArray)
						fieldsArray = nil
						fieldData = nil
					}
				}
			}
			handler.field_first_byte += time.Since(wait_a)
		case LINE_STATE_PROCESS_FIELD_SKIP_BYTES:
			wait_a := time.Now()
			skipBytes = append(skipBytes,d)
			found := NO_FOUND
			found,_ = handleTerminatedBy(handler,skipBytes,fields.Terminated,lines.TerminatedBy)
			if found == NO_FOUND {//does not find FIELD and LINE "TERMINATED BY"
				if handler.Status() != LINE_STATE_PROCESS_FIELD_SKIP_BYTES {
					panic(fmt.Errorf("skip bytes failed"))
				}
			}else if found == FIELD_END {
				//if len(fieldData) != 0, then a field with bytes
				//if len(fieldData) == 0, then a empty string field like ",,"
				//if len(fieldData) != 0 {
				//append fieldData into fieldsArray
				fieldsArray = append(fieldsArray,fieldData)
				fieldData = nil
				//}
				skipBytes = nil
			}else if found == LINE_END {
				if len(fieldData) != 0 {
					//append fieldData into fieldsArray
					fieldsArray = append(fieldsArray,fieldData)
					fieldData = nil
				}
				if len(fieldsArray) != 0 {
					lineArray = append(lineArray,fieldsArray)
					fieldsArray = nil
					fieldData = nil
				}
				skipBytes = nil
			}
			handler.field_skip_bytes += time.Since(wait_a)
		case LINE_STATE_PROCESS_FIELD_ENCLOSED://within "ENCLOSED BY"
			wait_a := time.Now()
			//check escape
			if d == fields.EscapedBy {//escape characters
				ok, nextByte := handler.Get()
				if !ok {
					break
				}
				switch nextByte {
				case fields.EnclosedBy://escape "ESCAPED BY" "ENCLOSED BY"
					fieldData = append(fieldData,nextByte)
				case fields.EscapedBy://escape "ESCAPED BY" "ESCAPED BY"
					fieldData = append(fieldData,nextByte)
				case 0://escape "ESCAPED BY" 0
					fieldData = append(fieldData,byte(0))
				case 'b'://escape "ESCAPED BY" b
					fieldData = append(fieldData,byte(8))
				case 'n'://escape "ESCAPED BY" n
					fieldData = append(fieldData,byte(10))
				case 'r'://escape "ESCAPED BY" r
					fieldData = append(fieldData,byte(13))
				case 't'://escape "ESCAPED BY" t
					fieldData = append(fieldData,byte(9))
				case 'Z'://escape "ESCAPED BY" Z
					fieldData = append(fieldData,byte(26))
				case 'N'://escape "ESCAPED BY" N -> NULL
					fieldData = append(fieldData,'N','U','L','L')
				default:
					logutil.Errorf("unsupported escape. escape by %v , byte %v",d,nextByte)
				}
			}else if d == fields.EnclosedBy {//get second "ENCLOSED BY"
				//two "ENCLOSED BY" define a field.
				//drop d
				//go to next field
				//++++
				//if len(fieldData) != 0 {
				//	//append fieldData into fieldsArray
				//	fieldsArray = append(fieldsArray,fieldData)
				//	fieldData = nil
				//}
				//handler.SwitchStatusTo(LINE_STATE_PROCESS_FIELD_FIRST_BYTE)
				handler.SwitchStatusTo(LINE_STATE_PROCESS_FIELD_SKIP_BYTES)
			}else{//normal character
				fieldData = append(fieldData,d)
			}
			handler.field_enclosed += time.Since(wait_a)
		case LINE_STATE_PROCESS_FIELD_WITHOUT_ENCLOSED://without "ENCLOSED BY"
			wait_a := time.Now()
			/*
			anything excludes "ESCAPED BY" and "TERMINATED BY" are bytes of field.
			 */
			if d == fields.EscapedBy {//escape characters
				ok, nextByte := handler.Get()
				if !ok {
					break
				}
				switch nextByte {
				case fields.EscapedBy://escape "ESCAPED BY" "ESCAPED BY"
					fieldData = append(fieldData,nextByte)
				case 0://escape "ESCAPED BY" 0
					fieldData = append(fieldData,byte(0))
				case 'b'://escape "ESCAPED BY" b
					fieldData = append(fieldData,byte(8))
				case 'n'://escape "ESCAPED BY" n
					fieldData = append(fieldData,byte(10))
				case 'r'://escape "ESCAPED BY" r
					fieldData = append(fieldData,byte(13))
				case 't'://escape "ESCAPED BY" t
					fieldData = append(fieldData,byte(9))
				case 'Z'://escape "ESCAPED BY" Z
					fieldData = append(fieldData,byte(26))
				case 'N'://escape "ESCAPED BY" N -> NULL
					fieldData = append(fieldData,'N','U','L','L')
				default:
					logutil.Errorf("unsupported escape. escape by %v , byte %v",d,nextByte)
				}
			}else {
				fieldData = append(fieldData,d)
			}
			found := NO_FOUND
			found,fieldData = handleTerminatedBy(handler,fieldData,fields.Terminated,lines.TerminatedBy)
			if found == FIELD_END {
				//if len(fieldData) != 0 {
					//append fieldData into fieldsArray
					fieldsArray = append(fieldsArray,fieldData)
					fieldData = nil
				//}
			}else if found == LINE_END {
				if len(fieldData) != 0 {
					//append fieldData into fieldsArray
					fieldsArray = append(fieldsArray,fieldData)
					fieldData = nil
				}
				if len(fieldsArray) != 0 {
					lineArray = append(lineArray,fieldsArray)
					fieldsArray = nil
					fieldData = nil
				}
			}
			handler.field_without += time.Since(wait_a)
		}
		handler.wait_switch += time.Since(wait_switch)
	}

	//do not forget the last field
	//if len(fieldData) != 0 {
	//	//append fieldData into fieldsArray
	//	fieldsArray = append(fieldsArray,fieldData)
	//	fieldData = nil
	//}
	handler.lineArray = lineArray
	handler.fieldsArray = fieldsArray
	handler.fieldData = fieldData
	handler.skipBytes = skipBytes
	handler.wait_loop += time.Since(wait_loop)
	return handler.lineArray,handler.fieldsArray,handler.fieldData
}

/*
split blocks into fields
*/
func splitFieldsImproved(handler *ParseLineHandler,blk *Block)([][][]byte,[][]byte,[]byte,error) {
	wait_split := time.Now()
	defer func() {
		handler.split_field += time.Since(wait_split)
	}()
	if !(handler.Status() == LINE_STATE_PROCESS_FIELD_FIRST_BYTE ||
		handler.Status() == LINE_STATE_PROCESS_FIELD_ENCLOSED ||
		handler.Status() == LINE_STATE_PROCESS_FIELD_WITHOUT_ENCLOSED ||
		handler.Status() == LINE_STATE_PROCESS_FIELD_SKIP_BYTES) {
		return nil,nil,nil,nil
	}
	fields := handler.fields
	lines := handler.lines

	if blk != nil {
		handler.PushBlock(blk)
	}

	dataLen := handler.GetDataLenInPartialBlocks()
	if dataLen == 0{
		return nil,nil,nil,nil
	}

	handler.split_before_loop += time.Since(wait_split)

	wait_loop := time.Now()
	/*
		reading bytes across blocks
		step 1 : read a byte
		step 2 : if the byte == "ENCLOSED BY", then anything behind it and before next
			"ENCLOSED BY" is a field.
		step 3 : if the byte != "ENCLOSED BY", then anything includes it ,behind it and before
			next "ENCLOSED BY" is a field.
	*/
	//lineArray := handler.lineArray
	fieldsArray := handler.fieldsArray
	fieldData := handler.fieldData
	skipBytes := handler.skipBytes

	var err error
	for  {
		ok,d := handler.Get()
		if !ok {
			break
		}
		//wait_switch := time.Now()
		if d == fields.EnclosedBy {
			//wait_a := time.Now()
			//field started with "ENCLOSED BY"
			//drop first "ENCLOSED BY" byte
			for  {
				ok,d = handler.Get()
				if !ok {
					break
				}

				if d == fields.EscapedBy {//escape characters
					ok, nextByte := handler.Get()
					if !ok {
						break
					}
					switch nextByte {
					case fields.EnclosedBy://escape "ESCAPED BY" "ENCLOSED BY"
						fieldData = append(fieldData,nextByte)
					case fields.EscapedBy://escape "ESCAPED BY" "ESCAPED BY"
						fieldData = append(fieldData,nextByte)
					case 0://escape "ESCAPED BY" 0
						fieldData = append(fieldData,byte(0))
					case 'b'://escape "ESCAPED BY" b
						fieldData = append(fieldData,byte(8))
					case 'n'://escape "ESCAPED BY" n
						fieldData = append(fieldData,byte(10))
					case 'r'://escape "ESCAPED BY" r
						fieldData = append(fieldData,byte(13))
					case 't'://escape "ESCAPED BY" t
						fieldData = append(fieldData,byte(9))
					case 'Z'://escape "ESCAPED BY" Z
						fieldData = append(fieldData,byte(26))
					case 'N'://escape "ESCAPED BY" N -> NULL
						fieldData = append(fieldData,'N','U','L','L')
					default:
						logutil.Errorf("unsupported escape. escape by %v , byte %v",d,nextByte)
					}
				}else if d == fields.EnclosedBy {//get second "ENCLOSED BY"
					//two "ENCLOSED BY" define a field.
					//drop d
					//go to next field
					//++++
					//if len(fieldData) != 0 {
					//	//append fieldData into fieldsArray
					//	fieldsArray = append(fieldsArray,fieldData)
					//	fieldData = nil
					//}
					//skip bytes
					for  {
						ok,d = handler.Get()
						if !ok {
							break
						}
						skipBytes = append(skipBytes,d)
						found := NO_FOUND
						found,_ = handleTerminatedBy(handler,skipBytes,fields.Terminated,lines.TerminatedBy)
						if found == FIELD_END {
							//if len(fieldData) != 0, then a field with bytes
							//if len(fieldData) == 0, then a empty string field like ",,"
							//if len(fieldData) != 0 {
							//append fieldData into fieldsArray
							handler.AppendField(fieldData)
							fieldData = fieldData[:0]
							//}
							skipBytes = skipBytes[:0]

							break
						}else if found == LINE_END {
							if len(fieldData) != 0 {
								//append fieldData into fieldsArray
								handler.AppendField(fieldData)
								fieldData = fieldData[:0]
							}
							//if len(fieldsArray) != 0 {
								err = handler.Newline()
								if err != nil{
									return nil, nil, nil, err
								}
								//fieldsArray = fieldsArray[:0]
								fieldData = fieldData[:0]
							//}
							skipBytes = skipBytes[:0]

							break
						}
					}

					//quit skip bytes
					break
				}else{//normal character
					fieldData = append(fieldData,d)
				}
			}
			//handler.field_enclosed += time.Since(wait_a)
		}else{
			//wait_a := time.Now()
			//without "ENCLOSED BY"
			for {
				/*
					anything excludes "ESCAPED BY" and "TERMINATED BY" are bytes of field.
				*/
				if d == fields.EscapedBy {//escape characters
					ok, nextByte := handler.Get()
					if !ok {
						break
					}
					switch nextByte {
					case fields.EscapedBy://escape "ESCAPED BY" "ESCAPED BY"
						fieldData = append(fieldData,nextByte)
					case 0://escape "ESCAPED BY" 0
						fieldData = append(fieldData,byte(0))
					case 'b'://escape "ESCAPED BY" b
						fieldData = append(fieldData,byte(8))
					case 'n'://escape "ESCAPED BY" n
						fieldData = append(fieldData,byte(10))
					case 'r'://escape "ESCAPED BY" r
						fieldData = append(fieldData,byte(13))
					case 't'://escape "ESCAPED BY" t
						fieldData = append(fieldData,byte(9))
					case 'Z'://escape "ESCAPED BY" Z
						fieldData = append(fieldData,byte(26))
					case 'N'://escape "ESCAPED BY" N -> NULL
						fieldData = append(fieldData,'N','U','L','L')
					default:
						logutil.Errorf("unsupported escape. escape by %v , byte %v",d,nextByte)
					}
				}else {
					fieldData = append(fieldData,d)
				}

				found := NO_FOUND
				found,fieldData = handleTerminatedBy(handler,fieldData,fields.Terminated,lines.TerminatedBy)
				if found == FIELD_END {
					//if len(fieldData) != 0 {
					//append fieldData into fieldsArray
					handler.AppendField(fieldData)
					fieldData = fieldData[:0]
					//}

					break
				}else if found == LINE_END {
					if len(fieldData) != 0 {
						//append fieldData into fieldsArray
						handler.AppendField(fieldData)
						fieldData = fieldData[:0]
					}
					//if len(fieldsArray) != 0 {
						err = handler.Newline()
						if err != nil{
							return nil, nil, nil, err
						}
						//fieldsArray = fieldsArray[:0]
						fieldData = fieldData[:0]
					//}

					break
				}

				ok,d = handler.Get()
				if !ok {
					break
				}
			}
			//handler.field_without += time.Since(wait_a)
		}
		//handler.wait_switch += time.Since(wait_switch)
	}

	//handler.PrintLineArray("++++>")

	//do not forget the last field
	//if len(fieldData) != 0 {
	//	//append fieldData into fieldsArray
	//	fieldsArray = append(fieldsArray,fieldData)
	//	fieldData = nil
	//}
	//handler.lineArray = lineArray
	handler.fieldsArray = fieldsArray
	handler.fieldData = fieldData
	handler.skipBytes = skipBytes
	handler.wait_loop += time.Since(wait_loop)
	return handler.lineArray,handler.fieldsArray,handler.fieldData,nil
}

/*
prepare batch
 */
func prepareBatch(handler *ParseLineHandler) error {
	relation := handler.tableHandler
	load := handler.load
	batchSize := handler.batchSize

	cols := relation.Attribute()
	attrName := make([]string,len(cols))
	tableName2ColumnId := make(map[string]int)
	for i, col := range cols {
		attrName[i] = col.Name
		tableName2ColumnId[col.Name] = i
	}

	//define the peer column for LOAD DATA's column list.
	var dataColumnId2TableColumnId []int = nil
	if len(load.ColumnList) == 0{
		dataColumnId2TableColumnId = make([]int,len(cols))
		for i := 0; i < len(cols); i++ {
			dataColumnId2TableColumnId[i] = i
		}
	}else{
		dataColumnId2TableColumnId = make([]int,len(load.ColumnList))
		for i, col := range load.ColumnList {
			switch realCol := col.(type) {
			case *tree.UnresolvedName:
				tid,ok := tableName2ColumnId[realCol.Parts[0]]
				if !ok {
					return fmt.Errorf("no such column %s in table %s ")
				}
				dataColumnId2TableColumnId[i] = tid
			case *tree.VarExpr:
				//NOTE:variable like '@abc' will be passed by.
				dataColumnId2TableColumnId[i] = -1
			default:
				return fmt.Errorf("unsupported column type %v",realCol)
			}
		}
	}

	handler.dataColumnId2TableColumnId = dataColumnId2TableColumnId

	batchData := batch.New(true,attrName)

	//alloc space for vector
	for i := 0; i < len(attrName); i++ {
		vec := vector.New(cols[i].Type)
		switch vec.Typ.Oid {
		case types.T_int8:
			vec.Col = make([]int8, batchSize)
		case types.T_int16:
			vec.Col = make([]int16, batchSize)
		case types.T_int32:
			vec.Col = make([]int32, batchSize)
		case types.T_int64:
			vec.Col = make([]int64, batchSize)
		case types.T_uint8:
			vec.Col = make([]uint8, batchSize)
		case types.T_uint16:
			vec.Col = make([]uint16, batchSize)
		case types.T_uint32:
			vec.Col = make([]uint32, batchSize)
		case types.T_uint64:
			vec.Col = make([]uint64, batchSize)
		case types.T_float32:
			vec.Col = make([]float32, batchSize)
		case types.T_float64:
			vec.Col = make([]float64, batchSize)
		case types.T_char, types.T_varchar:
			vBytes := &types.Bytes{
				Offsets: make([]uint32,batchSize),
				Lengths: make([]uint32,batchSize),
				Data: nil,
			}
			vec.Col = vBytes
		default:
			panic("unsupported vector type")
		}
		batchData.Vecs[i] = vec
	}
	handler.batchData = batchData



	return nil
}

/*
save parsed lines into the storage engine
 */
func saveParsedLinesToBatch(handler *ParseLineHandler) error {
	begin := time.Now()
	defer func() {
		fmt.Printf("-----saveParsedLinesToBach %s\n",time.Since(begin))
	}()
	//drop ignored lines
	if handler.lineCount < handler.load.IgnoredLines {
		ignoredCnt := handler.load.IgnoredLines - handler.lineCount
		ignoredCnt = MinUint64(ignoredCnt,uint64(len(handler.lineArray)))
		ignoredCnt = MaxUint64(ignoredCnt,0)

		handler.lineArray = handler.lineArray[ignoredCnt:]
		handler.lineCount += ignoredCnt
		handler.result.Skipped += ignoredCnt
	}

	countOfLineArray := len(handler.lineArray)
	if countOfLineArray == 0 {
		return nil
	}

	batchData := handler.batchData
	columnFLags := make([]byte,len(batchData.Vecs))
	fetchCnt := 0
	var err error
	allFetchCnt := 0


	row2col := time.Duration(0)
	fillBlank := time.Duration(0)
	toStorage := time.Duration(0)
	//write these lines
	for lineIdx := 0; lineIdx < countOfLineArray; lineIdx += fetchCnt {
		//fill batch
		fetchCnt = Min(countOfLineArray - lineIdx, handler.batchSize - handler.batchFilled)
		fetchLines := handler.lineArray[lineIdx : lineIdx + fetchCnt]

		/*
			row to column
		*/

		batchBegin := handler.batchFilled
		for i, line := range fetchLines {
			wait_a := time.Now()
			rowIdx := batchBegin + i
			//record missing column
			for k := 0; k < len(columnFLags); k++ {
				columnFLags[k] = 0
			}

			for j, field := range line {
				//fmt.Printf("data col %d : %v \n",j,field)
				//where will column j go ?
				colIdx := -1
				if j < len(handler.dataColumnId2TableColumnId) {
					colIdx = handler.dataColumnId2TableColumnId[j]
				}
				//drop this field
				if colIdx == -1 {
					continue
				}

				isNullOrEmpty := field == nil || len(field) == 0

				//put it into batch
				vec := batchData.Vecs[colIdx]

				//record colIdx
				columnFLags[colIdx] = 1

				//fmt.Printf("data set col %d : %v \n",j,field)

				switch vec.Typ.Oid {
				case types.T_int8:
					cols := vec.Col.([]int8)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,8)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = int8(d)
					}
				case types.T_int16:
					cols := vec.Col.([]int16)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,16)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = int16(d)
					}
				case types.T_int32:
					cols := vec.Col.([]int32)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,32)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = int32(d)
					}
				case types.T_int64:
					cols := vec.Col.([]int64)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,64)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = d
					}
				case types.T_uint8:
					cols := vec.Col.([]uint8)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,8)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = uint8(d)
					}
				case types.T_uint16:
					cols := vec.Col.([]uint16)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,16)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = uint16(d)
					}
				case types.T_uint32:
					cols := vec.Col.([]uint32)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,32)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = uint32(d)
					}
				case types.T_uint64:
					cols := vec.Col.([]uint64)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,64)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = uint64(d)
					}
				case types.T_float32:
					cols := vec.Col.([]float32)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseFloat(string(field),32)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = float32(d)
					}
				case types.T_float64:
					cols := vec.Col.([]float64)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						fs := string(field)
						//fmt.Printf("==== > field string [%s] \n",fs)
						d,err := strconv.ParseFloat(fs,64)
						if err != nil {
							logutil.Errorf("parse err:%v",d)
							d = 0
							//break
						}
						cols[rowIdx] = d
					}
				case types.T_char, types.T_varchar:
					vBytes :=vec.Col.(*types.Bytes)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
						vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
						vBytes.Lengths[rowIdx] = uint32(len(field))
					}else{
						vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
						vBytes.Data = append(vBytes.Data,field...)
						vBytes.Lengths[rowIdx] = uint32(len(field))
					}
				default:
					panic("unsupported oid")
				}
			}
			row2col += time.Since(wait_a)

			wait_b := time.Now()
			//the row does not have field
			for k := 0; k < len(columnFLags); k++ {
				if 0 == columnFLags[k] {
					vec := batchData.Vecs[k]
					switch vec.Typ.Oid {
					case types.T_char, types.T_varchar:
						vBytes :=vec.Col.(*types.Bytes)
						vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
						vBytes.Lengths[rowIdx] = uint32(0)
					}
					vec.Nsp.Add(uint64(rowIdx))
				}
			}
			fillBlank += time.Since(wait_b)
		}

		handler.lineCount += uint64(fetchCnt)
		handler.batchFilled = batchBegin + fetchCnt

		//if handler.batchFilled == handler.batchSize {
		//	minLen := math.MaxInt64
		//	maxLen := 0
		//	for _, vec := range batchData.Vecs {
		//		fmt.Printf("len %d type %d %s \n",vec.Length(),vec.Typ.Oid,vec.Typ.String())
		//		minLen = Min(vec.Length(),int(minLen))
		//		maxLen = Max(vec.Length(),int(maxLen))
		//	}
		//
		//	if minLen != maxLen{
		//		logutil.Errorf("vector length mis equal %d %d",minLen,maxLen)
		//		return fmt.Errorf("vector length mis equal %d %d",minLen,maxLen)
		//	}
		//}

		wait_c := time.Now()
		/*
		write batch into the engine
		*/
		//the second parameter must be FALSE here
		err = saveBatchToStorage(handler,false)
		if err != nil {
			logutil.Errorf("saveBatchToStorage failed. err:%v",err)
			return err
		}
		toStorage += time.Since(wait_c)

		allFetchCnt += fetchCnt
	}

	handler.row2col += row2col
	handler.fillBlank += fillBlank
	handler.toStorage += toStorage

	fmt.Printf("----- row2col %s fillBlank %s toStorage %s\n",
		row2col,fillBlank,toStorage)

	if allFetchCnt != countOfLineArray {
		return fmt.Errorf("allFetchCnt %d != countOfLineArray %d ",allFetchCnt,countOfLineArray)
	}
	handler.lineArray = handler.lineArray[:0]

	return nil
}

/*
save parsed lines into the storage engine
*/
func saveParsedLinesToBatchImprove(handler *ParseLineHandler, forceConvert bool) error {
	begin := time.Now()
	defer func() {
		fmt.Printf("-----saveParsedLinesToBatchImprove %s\n",time.Since(begin))
	}()

	countOfLineArray := handler.CountOfLines()
	if !forceConvert {
		if countOfLineArray != handler.capacityOfLineAarray {
			panic("-----write a batch")
		}
	}

	batchData := handler.batchData
	columnFLags := make([]byte,len(batchData.Vecs))
	fetchCnt := 0
	var err error
	allFetchCnt := 0


	row2col := time.Duration(0)
	fillBlank := time.Duration(0)
	toStorage := time.Duration(0)
	//write batch of  lines
	//for lineIdx := 0; lineIdx < countOfLineArray; lineIdx += fetchCnt {
		//fill batch
		fetchCnt = countOfLineArray
		//fmt.Printf("-----fetchCnt %d len(lineArray) %d\n",fetchCnt,len(handler.lineArray))
		fetchLines := handler.lineArray[:fetchCnt]

		/*
			row to column
		*/

		batchBegin := handler.batchFilled
		for i, line := range fetchLines {
			//fmt.Printf("line %d %v \n",i,line)
			wait_a := time.Now()
			rowIdx := batchBegin + i
			//record missing column
			for k := 0; k < len(columnFLags); k++ {
				columnFLags[k] = 0
			}

			for j, field := range line {
				//fmt.Printf("data col %d : %v \n",j,field)
				//where will column j go ?
				colIdx := -1
				if j < len(handler.dataColumnId2TableColumnId) {
					colIdx = handler.dataColumnId2TableColumnId[j]
				}
				//drop this field
				if colIdx == -1 {
					continue
				}

				isNullOrEmpty := field == nil || len(field) == 0

				//put it into batch
				vec := batchData.Vecs[colIdx]

				//record colIdx
				columnFLags[colIdx] = 1

				//fmt.Printf("data set col %d : %v \n",j,field)

				switch vec.Typ.Oid {
				case types.T_int8:
					cols := vec.Col.([]int8)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,8)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = int8(d)
					}
				case types.T_int16:
					cols := vec.Col.([]int16)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,16)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = int16(d)
					}
				case types.T_int32:
					cols := vec.Col.([]int32)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = int32(d)
					}
				case types.T_int64:
					cols := vec.Col.([]int64)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = d
					}
				case types.T_uint8:
					cols := vec.Col.([]uint8)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,8)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = uint8(d)
					}
				case types.T_uint16:
					cols := vec.Col.([]uint16)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,16)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = uint16(d)
					}
				case types.T_uint32:
					cols := vec.Col.([]uint32)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = uint32(d)
					}
				case types.T_uint64:
					cols := vec.Col.([]uint64)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseInt(string(field),10,64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = uint64(d)
					}
				case types.T_float32:
					cols := vec.Col.([]float32)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						d,err := strconv.ParseFloat(string(field),32)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = float32(d)
					}
				case types.T_float64:
					cols := vec.Col.([]float64)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
					}else{
						fs := string(field)
						//fmt.Printf("==== > field string [%s] \n",fs)
						d,err := strconv.ParseFloat(fs,64)
						if err != nil {
							logutil.Errorf("parse field[%v] err:%v",field,err)
							d = 0
							//break
						}
						cols[rowIdx] = d
					}
				case types.T_char, types.T_varchar:
					vBytes :=vec.Col.(*types.Bytes)
					if isNullOrEmpty {
						vec.Nsp.Add(uint64(rowIdx))
						vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
						vBytes.Lengths[rowIdx] = uint32(len(field))
					}else{
						vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
						vBytes.Data = append(vBytes.Data,field...)
						vBytes.Lengths[rowIdx] = uint32(len(field))
					}
				default:
					panic("unsupported oid")
				}
			}
			row2col += time.Since(wait_a)

			wait_b := time.Now()
			//the row does not have field
			for k := 0; k < len(columnFLags); k++ {
				if 0 == columnFLags[k] {
					vec := batchData.Vecs[k]
					switch vec.Typ.Oid {
					case types.T_char, types.T_varchar:
						vBytes :=vec.Col.(*types.Bytes)
						vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
						vBytes.Lengths[rowIdx] = uint32(0)
					}
					vec.Nsp.Add(uint64(rowIdx))
				}
			}
			fillBlank += time.Since(wait_b)
		}

		handler.lineCount += uint64(fetchCnt)
		handler.batchFilled = batchBegin + fetchCnt

		//if handler.batchFilled == handler.batchSize {
		//	minLen := math.MaxInt64
		//	maxLen := 0
		//	for _, vec := range batchData.Vecs {
		//		fmt.Printf("len %d type %d %s \n",vec.Length(),vec.Typ.Oid,vec.Typ.String())
		//		minLen = Min(vec.Length(),int(minLen))
		//		maxLen = Max(vec.Length(),int(maxLen))
		//	}
		//
		//	if minLen != maxLen{
		//		logutil.Errorf("vector length mis equal %d %d",minLen,maxLen)
		//		return fmt.Errorf("vector length mis equal %d %d",minLen,maxLen)
		//	}
		//}

		wait_c := time.Now()
		/*
			write batch into the engine
		*/
		//the second parameter must be FALSE here
		err = saveBatchToStorage(handler,forceConvert)
		if err != nil {
			logutil.Errorf("saveBatchToStorage failed. err:%v",err)
			return err
		}
		toStorage += time.Since(wait_c)

		allFetchCnt += fetchCnt
	//}

	handler.row2col += row2col
	handler.fillBlank += fillBlank
	handler.toStorage += toStorage

	fmt.Printf("----- row2col %s fillBlank %s toStorage %s\n",
		row2col,fillBlank,toStorage)

	if allFetchCnt != countOfLineArray {
		return fmt.Errorf("allFetchCnt %d != countOfLineArray %d ",allFetchCnt,countOfLineArray)
	}

	return nil
}

func saveParsedLinesToBatchSimdCsv(handler *ParseLineHandler, forceConvert bool) error {
	begin := time.Now()
	defer func() {
		fmt.Printf("-----saveParsedLinesToBatchSimdCsv %s\n",time.Since(begin))
	}()

	countOfLineArray := handler.lineIdx
	if !forceConvert {
		if countOfLineArray != handler.batchSize {
			panic("-----write a batch")
		}
	}

	batchData := handler.batchData
	columnFLags := make([]byte,len(batchData.Vecs))
	fetchCnt := 0
	var err error
	allFetchCnt := 0


	row2col := time.Duration(0)
	fillBlank := time.Duration(0)
	toStorage := time.Duration(0)
	//write batch of  lines
	//for lineIdx := 0; lineIdx < countOfLineArray; lineIdx += fetchCnt {
	//fill batch
	fetchCnt = countOfLineArray
	//fmt.Printf("-----fetchCnt %d len(lineArray) %d\n",fetchCnt,len(handler.lineArray))
	fetchLines := handler.simdCsvLineArray[:fetchCnt]

	/*
		row to column
	*/

	batchBegin := handler.batchFilled
	for i, line := range fetchLines {
		//fmt.Printf("line %d %v \n",i,line)
		wait_a := time.Now()
		rowIdx := batchBegin + i
		//record missing column
		for k := 0; k < len(columnFLags); k++ {
			columnFLags[k] = 0
		}

		for j, field := range line {
			//fmt.Printf("data col %d : %v \n",j,field)
			//where will column j go ?
			colIdx := -1
			if j < len(handler.dataColumnId2TableColumnId) {
				colIdx = handler.dataColumnId2TableColumnId[j]
			}
			//drop this field
			if colIdx == -1 {
				continue
			}

			isNullOrEmpty := len(field) == 0

			//put it into batch
			vec := batchData.Vecs[colIdx]

			//record colIdx
			columnFLags[colIdx] = 1

			//fmt.Printf("data set col %d : %v \n",j,field)

			switch vec.Typ.Oid {
			case types.T_int8:
				cols := vec.Col.([]int8)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseInt(string(field),10,8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = int8(d)
				}
			case types.T_int16:
				cols := vec.Col.([]int16)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseInt(string(field),10,16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = int16(d)
				}
			case types.T_int32:
				cols := vec.Col.([]int32)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseInt(string(field),10,32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = int32(d)
				}
			case types.T_int64:
				cols := vec.Col.([]int64)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseInt(string(field),10,64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = d
				}
			case types.T_uint8:
				cols := vec.Col.([]uint8)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseInt(string(field),10,8)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = uint8(d)
				}
			case types.T_uint16:
				cols := vec.Col.([]uint16)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseInt(string(field),10,16)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = uint16(d)
				}
			case types.T_uint32:
				cols := vec.Col.([]uint32)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseInt(string(field),10,32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = uint32(d)
				}
			case types.T_uint64:
				cols := vec.Col.([]uint64)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseInt(string(field),10,64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = uint64(d)
				}
			case types.T_float32:
				cols := vec.Col.([]float32)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					d,err := strconv.ParseFloat(string(field),32)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = float32(d)
				}
			case types.T_float64:
				cols := vec.Col.([]float64)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					fs := string(field)
					//fmt.Printf("==== > field string [%s] \n",fs)
					d,err := strconv.ParseFloat(fs,64)
					if err != nil {
						logutil.Errorf("parse field[%v] err:%v",field,err)
						d = 0
						//break
					}
					cols[rowIdx] = d
				}
			case types.T_char, types.T_varchar:
				vBytes :=vec.Col.(*types.Bytes)
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Lengths[rowIdx] = uint32(len(field))
				}else{
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Data = append(vBytes.Data,field...)
					vBytes.Lengths[rowIdx] = uint32(len(field))
				}
			default:
				panic("unsupported oid")
			}
		}
		row2col += time.Since(wait_a)

		wait_b := time.Now()
		//the row does not have field
		for k := 0; k < len(columnFLags); k++ {
			if 0 == columnFLags[k] {
				vec := batchData.Vecs[k]
				switch vec.Typ.Oid {
				case types.T_char, types.T_varchar:
					vBytes :=vec.Col.(*types.Bytes)
					vBytes.Offsets[rowIdx] = uint32(len(vBytes.Data))
					vBytes.Lengths[rowIdx] = uint32(0)
				}
				vec.Nsp.Add(uint64(rowIdx))
			}
		}
		fillBlank += time.Since(wait_b)
	}

	handler.lineCount += uint64(fetchCnt)
	handler.batchFilled = batchBegin + fetchCnt

	//if handler.batchFilled == handler.batchSize {
	//	minLen := math.MaxInt64
	//	maxLen := 0
	//	for _, vec := range batchData.Vecs {
	//		fmt.Printf("len %d type %d %s \n",vec.Length(),vec.Typ.Oid,vec.Typ.String())
	//		minLen = Min(vec.Length(),int(minLen))
	//		maxLen = Max(vec.Length(),int(maxLen))
	//	}
	//
	//	if minLen != maxLen{
	//		logutil.Errorf("vector length mis equal %d %d",minLen,maxLen)
	//		return fmt.Errorf("vector length mis equal %d %d",minLen,maxLen)
	//	}
	//}

	wait_c := time.Now()
	/*
		write batch into the engine
	*/
	//the second parameter must be FALSE here
	err = saveBatchToStorage(handler,forceConvert)
	if err != nil {
		logutil.Errorf("saveBatchToStorage failed. err:%v",err)
		return err
	}
	toStorage += time.Since(wait_c)

	allFetchCnt += fetchCnt
	//}

	handler.row2col += row2col
	handler.fillBlank += fillBlank
	handler.toStorage += toStorage

	fmt.Printf("----- row2col %s fillBlank %s toStorage %s\n",
		row2col,fillBlank,toStorage)

	if allFetchCnt != countOfLineArray {
		return fmt.Errorf("allFetchCnt %d != countOfLineArray %d ",allFetchCnt,countOfLineArray)
	}

	return nil
}


/*
save batch to storage.
when force is true, batchsize will be changed.
 */
func saveBatchToStorage(handler *ParseLineHandler,force bool) error {
	if handler.batchFilled == handler.batchSize{
		//for _, vec := range handler.batchData.Vecs {
		//	fmt.Printf("len %d type %d %s \n",vec.Length(),vec.Typ.Oid,vec.Typ.String())
		//}
		wait_a := time.Now()
		err := handler.tableHandler.Write(handler.timestamp,handler.batchData)
		if err != nil {
			logutil.Errorf("write failed. err: %v",err)
			return err
		}

		handler.writeBatch += time.Since(wait_a)

		handler.result.Records += uint64(handler.batchSize)

		wait_b := time.Now()
		//clear batch
		//clear vector.nulls.Nulls
		for _, vec := range handler.batchData.Vecs {
			vec.Nsp = &nulls.Nulls{}
			switch vec.Typ.Oid {
			case types.T_char, types.T_varchar:
				vBytes := vec.Col.(*types.Bytes)
				vBytes.Data = vBytes.Data[:0]
			}
		}
		handler.batchFilled = 0

		handler.resetBatch += time.Since(wait_b)
	}else{
		if force {
			//first, remove redundant rows at last
			needLen := handler.batchFilled
			if needLen > 0{
				logutil.Infof("needLen: %d batchSize %d", needLen, handler.batchSize)
				for _, vec := range handler.batchData.Vecs {
					//fmt.Printf("needLen %d %d type %d %s \n",needLen,i,vec.Typ.Oid,vec.Typ.String())
					//remove nulls.NUlls
					for j := uint64(handler.batchFilled); j < uint64(handler.batchSize); j++ {
						vec.Nsp.Del(j)
					}
					//remove row
					switch vec.Typ.Oid {
					case types.T_int8:
						cols := vec.Col.([]int8)
						vec.Col = cols[:needLen]
					case types.T_int16:
						cols := vec.Col.([]int16)
						vec.Col = cols[:needLen]
					case types.T_int32:
						cols := vec.Col.([]int32)
						vec.Col = cols[:needLen]
					case types.T_int64:
						cols := vec.Col.([]int64)
						vec.Col = cols[:needLen]
					case types.T_uint8:
						cols := vec.Col.([]uint8)
						vec.Col = cols[:needLen]
					case types.T_uint16:
						cols := vec.Col.([]uint16)
						vec.Col = cols[:needLen]
					case types.T_uint32:
						cols := vec.Col.([]uint32)
						vec.Col = cols[:needLen]
					case types.T_uint64:
						cols := vec.Col.([]uint64)
						vec.Col = cols[:needLen]
					case types.T_float32:
						cols := vec.Col.([]float32)
						vec.Col = cols[:needLen]
					case types.T_float64:
						cols := vec.Col.([]float64)
						vec.Col = cols[:needLen]
					case types.T_char, types.T_varchar://bytes is different
						vBytes := vec.Col.(*types.Bytes)
						//fmt.Printf("saveBatchToStorage before data %s \n",vBytes.String())
						if len(vBytes.Offsets) > needLen{
							vec.Col = vBytes.Window(0, needLen)
						}

						//fmt.Printf("saveBatchToStorage after data %s \n",vBytes.String())
					}
				}

				err := handler.tableHandler.Write(handler.timestamp, handler.batchData)
				if err != nil {
					logutil.Errorf("write failed. err:%v \n", err)
					return err
				}
			}

			handler.result.Records += uint64(needLen)
		}
	}
	return nil
}

/*
process block
 */
func processBlock(handler *ParseLineHandler,blk *Block) error {
	lines := handler.lines
	//fields := handler.fields
	done := false
	for !done {
		switch handler.Status() {
		case LINE_STATE_PROCESS_PREFIX:
			wait_a := time.Now()
			findStringAmongBlocks(handler,blk,lines.StartingBy,LINE_STATE_PROCESS_FIELD_FIRST_BYTE)
			//block has been pushed already
			blk = nil
			handler.prefix += time.Since(wait_a)
		case LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
			LINE_STATE_PROCESS_FIELD_ENCLOSED,
			LINE_STATE_PROCESS_FIELD_WITHOUT_ENCLOSED,
			LINE_STATE_PROCESS_FIELD_SKIP_BYTES:
			wait_a := time.Now()
			_,_,_,err := splitFieldsImproved(handler,blk)
			if err != nil {
				return err
			}
			if len(handler.lineArray) != 0 {
				if handler.packLine != nil {
					handler.packLine(handler.lineArray)
				}
			}
			done = true
			handler.process_field += time.Since(wait_a)
		default:
			panic(fmt.Errorf("unsupported line state %d",handler.Status()))
		}

	}

	return nil
}

/*
LoadLoop reads data from stream, extracts the fields, and saves into the table
 */
func (mce *MysqlCmdExecutor) LoadLoop (load *tree.Load, dbHandler engine.Database,tableHandler engine.Relation) (*LoadResult,error){
	var err error
	ses := mce.routine.GetSession()

	begin:=  time.Now()
	defer func() {
		fmt.Printf("-----load loop exit %s\n",time.Since(begin))
	}()

	result := &LoadResult{}

	/*
	step1 : read block from file
	 */
	dataFile,err := os.Open(load.File)
	if err != nil {
		logutil.Errorf("open file failed. err:%v",err)
		return nil, err
	}
	defer func() {
		err := dataFile.Close()
		if err != nil{
			logutil.Errorf("close file failed. err:%v",err)
		}
	}()

	processTime := time.Now()
	wait_block := time.Duration(0)
	process_blcok := time.Duration(0)
	pop_block := time.Duration(0)
	save_batch := time.Duration(0)

	//add profile
	cpuProf,err := os.Create("load_profile")
	if err != nil {
		logutil.Errorf("create cpu profile")
		return nil,err
	}
	pprof.StartCPUProfile(cpuProf)

	if ses.Pu.SV.GetLoadDataParserType() == 0 {
		//simdcsv
		handler := &ParseLineHandler{
			load: load,
			fields:       load.Fields,
			lines:        load.Lines,
			lineIdx: 0,
			simdCsvLineArray: make([][]string, int(ses.Pu.SV.GetBatchSizeInLoadData())),
			simdCsvReader: simdcsv.NewReaderWithOptions(dataFile,
				rune(load.Fields.Terminated[0]),
				'#',
				false,
				false),
				simdCsvLineOutChan: make(chan simdcsv.LineOut,100 * int(ses.Pu.SV.GetBatchSizeInLoadData())),
				simdCsvLineOutRoutineClose: &CloseFlag{},
			dbHandler: dbHandler,
			tableHandler: tableHandler,
			lineCount: 0,
			batchSize: int(ses.Pu.SV.GetBatchSizeInLoadData()),
			result: result,
			row2col: 0,
			fillBlank: 0,
			toStorage: 0,
			writeBatch: 0,
			resetBatch: 0,
		}

		prepareTime := time.Now()
		//allocate batch space
		err = prepareBatch(handler)
		if err != nil {
			return nil, err
		}

		fmt.Printf("-----prepare batch %s\n",time.Since(prepareTime))
		wg := sync.WaitGroup{}
		go func() {
			wg.Add(1)
			err = handler.getLineOutFromSimdCsvRoutine(&wg)
			if err != nil {
				logutil.Errorf("get line from simdcsv failed. err:%v",err)
			}
		}()

		wait_b := time.Now()
		err = handler.simdCsvReader.ReadLoop(handler.simdCsvLineOutChan, nil)
		if err != nil {
			return result, err
		}
		process_blcok += time.Since(wait_b)

		wg.Wait()


		fmt.Printf("-----total row2col %s fillBlank %s toStorage %s\n",
			handler.row2col,handler.fillBlank,handler.toStorage)
		fmt.Printf("-----write batch %s reset batch %s\n",
			handler.writeBatch,handler.resetBatch)
		fmt.Printf("----- simdcsv end %s " +
			"stage1_first_chunk %s stage1_end %s " +
			"stage2_first_chunkinfo - [begin end] [%s %s ] [%s %s ] [%s %s ] " +
			"readLoop_first_records %s \n",
			handler.simdCsvReader.End,
			handler.simdCsvReader.Stage1_first_chunk,
			handler.simdCsvReader.Stage1_end,
			handler.simdCsvReader.Stage2_first_chunkinfo[0],
			handler.simdCsvReader.Stage2_end[0],
			handler.simdCsvReader.Stage2_first_chunkinfo[1],
			handler.simdCsvReader.Stage2_end[1],
			handler.simdCsvReader.Stage2_first_chunkinfo[2],
			handler.simdCsvReader.Stage2_end[2],
			handler.simdCsvReader.ReadLoop_first_records,
			)
		fmt.Printf("-----call_back %s " +
			"process_block - callback %s " +
			"asyncChan %s asyncChanLoop %s asyncChan - asyncChanLoop %s " +
			"csvLineArray1 %s csvLineArray2 %s\n",
			handler.callback,
			process_blcok - handler.callback,
			handler.asyncChan,
			handler.asyncChanLoop,
			handler.asyncChan -	handler.asyncChanLoop,
			handler.csvLineArray1,
			handler.csvLineArray2,
			)


		lastSave := time.Now()
		//the second parameter must be TRUE here
		err = saveParsedLinesToBatchSimdCsv(handler,true)
		if err != nil {
			return nil, err
		}

		fmt.Printf("----- lastSave %s\n",time.Since(lastSave))
		fmt.Printf("-----process time %s \n",time.Since(processTime))
		pprof.StopCPUProfile()
	}else if ses.Pu.SV.GetLoadDataParserType() == 1 {
		handler := &ParseLineHandler{
			status:       LINE_STATE_PROCESS_PREFIX,
			partialBlocks: nil,
			load: load,
			fields:       load.Fields,
			lines:        load.Lines,
			dbHandler: dbHandler,
			tableHandler: tableHandler,
			lineCount: 0,
			capacityOfLineAarray: int(ses.Pu.SV.GetBatchSizeInLoadData()),
			lineIdx: 0,
			colIdx: 0,
			batchSize: int(ses.Pu.SV.GetBatchSizeInLoadData()),
			result: result,
			row2col: 0,
			fillBlank: 0,
			toStorage: 0,
			writeBatch: 0,
			resetBatch: 0,
		}

		prepareTime := time.Now()
		//allocate batch space
		err = prepareBatch(handler)
		if err != nil {
			return nil, err
		}

		fmt.Printf("-----prepare batch %s\n",time.Since(prepareTime))

		//handwritten
		initBlock := time.Now()
		//create blocks for loading data
		blockCnt := int(ses.Pu.SV.GetBlockCountInLoadData())
		blockSize := int(ses.Pu.SV.GetBlockSizeInLoadData())
		//block has no data, empty block
		blockPoolWithoutData := NewBlockPool(blockCnt,blockSize)
		//block has data loaded from file
		blockChannel := make(chan *Block,blockCnt)
		closeLoadFile := &CloseFlag{}
		closeProcessBlock := &CloseFlag{}

		fmt.Printf("----- init block %s\n",time.Since(initBlock))

		//load data from file asynchronously
		go func() {
			err = loadDataFromFileRoutine(closeLoadFile,dataFile,blockPoolWithoutData,blockChannel)
			if err != nil {
				logutil.Errorf("load data filed.err:%v",err)
			}

			fmt.Printf("exit load data routine\n")
		}()

		cnt := 0
		alllineCnt := 0

		closeProcessBlock.Open()


		//process block
		for closeProcessBlock.IsOpened(){
			fmt.Printf("-----wait block\n")

			wait_a := time.Now()
			blk,ok := <- blockChannel
			wait_block += time.Since(wait_a)

			if !ok {
				break
			}
			if blk == nil {
				break
			}
			cnt++
			fmt.Printf("process blk %d , count of used block %d\n",blk.id,cnt)

			wait_b := time.Now()
			//process block
			//TODO:row to column, put into engine
			err = processBlock(handler,blk)
			if err != nil {
				logutil.Errorf("processBlock failed. err:%v",err)
			}
			process_blcok += time.Since(wait_b)

			alllineCnt += len(handler.lineArray)
			fmt.Printf("get lines %d, all line count %d \n",len(handler.lineArray),alllineCnt)

			wait_c := time.Now()
			//recycle poped blocks
			popedBlocks := handler.GetPopedBlocks()
			if len(popedBlocks) != 0{
				for _, ppBlk := range popedBlocks {
					blockPoolWithoutData.PutBlock(ppBlk)
				}
				handler.ClearPopedBlocks()
			}

			pop_block += time.Since(wait_c)

			//wait_d := time.Now()
			///*
			//	step6 : append column to a batch
			//*/
			//err = saveParsedLinesToBatch(handler)
			//if err != nil {
			//	logutil.Errorf("saveParsedLinesToBatch failed. err:%v",err)
			//}
			//save_batch += time.Since(wait_d)
		}
		fmt.Printf("-----total row2col %s fillBlank %s toStorage %s\n",
			handler.row2col,handler.fillBlank,handler.toStorage)
		fmt.Printf("-----write batch %s reset batch %s\n",
			handler.writeBatch,handler.resetBatch)
		fmt.Printf("-----prefix %s skip_unused_bytes %s process_field %s split_field %s split_before_loop %s wait_loop %s " +
			" wait_loop - toStorage %s handler_get %s" +
			" wait_switch %s field_first_byte %s field_enclosed %s field_without_enclosed %s field_skip_bytes %s\n",
			handler.prefix,handler.skip_bytes,
			handler.process_field,handler.split_field,
			handler.split_before_loop,
			handler.wait_loop,
			handler.wait_loop - handler.toStorage,
			handler.handler_get,
			handler.wait_switch,
			handler.field_first_byte,handler.field_enclosed,
			handler.field_without,handler.field_skip_bytes)
		fmt.Printf("-----wait_block %s process_block %s pop_block %s save_batch %s\n",
			wait_block,process_blcok,pop_block,save_batch)
		fmt.Printf("-----process time %s \n",time.Since(processTime))

		lastSave := time.Now()
		//the second parameter must be TRUE here
		err = saveParsedLinesToBatchImprove(handler,true)
		if err != nil {
			return nil, err
		}

		fmt.Printf("----- lastSave %s\n",time.Since(lastSave))
		pprof.StopCPUProfile()

		//close something
		closeLoadFile.Close()
		closeProcessBlock.Close()
		close(blockChannel)
		blockPoolWithoutData.Release()
	}

	return result, nil
}