package frontend

import (
	"bytes"
	"fmt"
	"io"
	"matrixone/pkg/container/batch"
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/logutil"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm/engine"
	"os"
	"strconv"
	"sync"
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
	b.AssertLegal()
	return b.writeIdx - b.readIdx
}

func (b *Block) GetData() []byte {
	return b.data
}

/*
get the slice [readIdx,writeIdx]
 */
func (b *Block) GetDataSlice() []byte{
	b.AssertLegal()
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
	for close.IsOpened() {
		blk := pool.GetBlock()
		if blk == nil {
			continue
		}

		reads,err = dataFile.Read(blk.GetData())
		if reads == 0 && err == io.EOF{
			//end of file
			//write a nil to notice that reading data is over
			outChan <- nil
			return nil
		}else if err != nil {
			outChan <- nil
			return err
		}
		blk.SetIdx(0,reads)

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

	//storage
	dbHandler engine.Database
	tableHandler engine.Relation

	//result of load
	result *LoadResult

	//for debug
	packLine func([][][]byte)
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

/*
read next byte.
readidx changes in the block.
 */
func (plh *ParseLineHandler) Get() (bool,byte) {
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
		ret := bytes.Index(fieldData,[]byte(fieldTerminatedBy))
		if ret != -1 {//find "TERMINATED BY"
			//remove "TERMINATED BY" part
			//state changes
			fieldData = fieldData[:fieldLen - lenOfFieldTerminated]
			handler.SwitchStatusTo(LINE_STATE_PROCESS_FIELD_FIRST_BYTE)
			return FIELD_END, fieldData
		}
	}
	if fieldLen >= lenOfLineTerminated {//check FIELDS "TERMINATED BY"
		ret := bytes.Index(fieldData,[]byte(lineTerminatedBy))
		if ret != -1 {//find "TERMINATED BY"
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

		switch handler.Status() {
		case LINE_STATE_PROCESS_FIELD_FIRST_BYTE://first byte
			//if fieldData != nil {
			//	//append fieldData into fieldsArray
			//	fieldsArray = append(fieldsArray,fieldData)
			//	fieldData = nil
			//}
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
		case LINE_STATE_PROCESS_FIELD_SKIP_BYTES:
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
		case LINE_STATE_PROCESS_FIELD_ENCLOSED://within "ENCLOSED BY"
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
		case LINE_STATE_PROCESS_FIELD_WITHOUT_ENCLOSED://without "ENCLOSED BY"
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
		}
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
	return handler.lineArray,handler.fieldsArray,handler.fieldData
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
			charArrary := make([][]byte,batchSize)
			vbytes := &types.Bytes{}
			err := vbytes.Append(charArrary)
			if err != nil {
				return err
			}
			vec.Col = vbytes

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

	//batch is full.
	if handler.batchFilled >= handler.batchSize {
		return nil
	}

	fetchCnt := Min(countOfLineArray,handler.batchSize - handler.batchFilled)
	fetchLines := handler.lineArray[:fetchCnt]
	handler.lineArray = handler.lineArray[fetchCnt:]
	batchData := handler.batchData

	/*
	fill the batch.
	row to column
	 */
	batchBegin := handler.batchFilled
	for i, line := range fetchLines {
		rowIdx := batchBegin + i
		for j, field := range line {
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
					d,err := strconv.ParseFloat(string(field),64)
					if err != nil {
						logutil.Errorf("parse err:%v",d)
						d = 0
						//break
					}
					cols[rowIdx] = d
				}
			case types.T_char, types.T_varchar:
				if isNullOrEmpty {
					vec.Nsp.Add(uint64(rowIdx))
				}else{
					err := vec.Col.(*types.Bytes).Append([][]byte{field})
					if err != nil {
						logutil.Errorf("parse err:%v",err)
						//TODO:
					}
				}
			default:
				panic("unsupported oid")
			}
		}
		handler.lineCount++
	}

	handler.batchFilled = batchBegin + fetchCnt

	return nil
}

/*
save batch to storage.
when force is true, batchsize will be changed.
 */
func saveBatchToStorage(handler *ParseLineHandler,force bool) error {
	if handler.batchFilled == handler.batchSize{
		err := handler.tableHandler.Write(handler.timestamp,handler.batchData)
		if err != nil {
			logutil.Errorf("write failed. err: %v",err)
			return err
		}

		handler.result.Records += uint64(handler.batchSize)

		//clear batch
		//clear vector.nulls.Nulls
		for _, vec := range handler.batchData.Vecs {
			vec.Nsp = &nulls.Nulls{}
			switch vec.Typ.Oid {
			case types.T_char, types.T_varchar:
				vec.Col.(*types.Bytes).Reset()
			}
		}
		handler.batchFilled = 0
	}else{
		if force {
			//first, remove redundant rows at last
			needLen := handler.batchFilled
			for i, vec := range handler.batchData.Vecs {
				fmt.Printf("%d type %d %s \n",i,vec.Typ.Oid,vec.Typ.String())
				//remove nulls.NUlls
				for i := uint64(handler.batchFilled); i < uint64(handler.batchSize); i++ {
					vec.Nsp.Del(i)
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
				case types.T_char, types.T_varchar:
					vec.Col = vec.Col.(*types.Bytes).Window(0,needLen)
					//vec.Col = &types.Bytes{}
				}
			}

			err := handler.tableHandler.Write(handler.timestamp,handler.batchData)
			if err != nil {
				return err
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
			findStringAmongBlocks(handler,blk,lines.StartingBy,LINE_STATE_PROCESS_FIELD_FIRST_BYTE)
			//block has been pushed already
			blk = nil
		case LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
			LINE_STATE_PROCESS_FIELD_ENCLOSED,
			LINE_STATE_PROCESS_FIELD_WITHOUT_ENCLOSED,
			LINE_STATE_PROCESS_FIELD_SKIP_BYTES:
			splitFields(handler,blk)
			if len(handler.lineArray) != 0 {
				if handler.packLine != nil {
					handler.packLine(handler.lineArray)
				}
			}
			done = true
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
	result := &LoadResult{}
	handler := &ParseLineHandler{
		status:       LINE_STATE_PROCESS_PREFIX,
		partialBlocks: nil,
		load: load,
		fields:       load.Fields,
		lines:        load.Lines,
		dbHandler: dbHandler,
		tableHandler: tableHandler,
		lineCount: 0,
		batchSize: 100,
		result: result,
	}

	//allocate batch space
	err = prepareBatch(handler)
	if err != nil {
		return nil, err
	}

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

	//create blocks for loading data
	blockCnt := 10
	blockSize := 2 * 1024 * 1024
	//block has no data, empty block
	blockPoolWithoutData := NewBlockPool(blockCnt,blockSize)
	//block has data loaded from file
	blockChannel := make(chan *Block,blockCnt)
	close := &CloseFlag{}

	//load data from file asynchronously
	go func() {
		err := loadDataFromFileRoutine(close,dataFile,blockPoolWithoutData,blockChannel)
		if err != nil {
			logutil.Errorf("load data filed.err:%v",err)
		}
	}()

	//process block
	for {
		blk,ok := <- blockChannel
		if !ok {
			break
		}
		if blk == nil {
			break
		}

		//process block
		//TODO:row to column, put into engine
		err = processBlock(handler,blk)
		if err != nil {
			logutil.Errorf("processBlock failed. err:%v",err)
		}

		//recycle poped blocks
		popedBlocks := handler.GetPopedBlocks()
		if len(popedBlocks) != 0{
			for _, ppBlk := range popedBlocks {
				blockPoolWithoutData.PutBlock(ppBlk)
			}
			handler.ClearPopedBlocks()
		}

		/*
			step6 : append column to a batch
		*/
		err = saveParsedLinesToBatch(handler)
		if err != nil {
			logutil.Errorf("saveParsedLinesToBatch failed. err:%v",err)
		}

		/*
			step7 : write batch into the engine
		*/
		//the second parameter must be FALSE here
		err = saveBatchToStorage(handler,false)
		if err != nil {
			logutil.Errorf("saveBatchToStorage failed. err:%v",err)
		}
	}

	//the second parameter must be TRUE here
	err = saveBatchToStorage(handler,true)
	if err != nil {
		logutil.Errorf("saveBatchToStorage failed. err:%v",err)
	}

	return result, nil
}