package frontend

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/require"
	"matrixone/pkg/logutil"
	"matrixone/pkg/sql/tree"
	"matrixone/pkg/vm/engine/aoe/common/codec"
	"os"
	"reflect"
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
		if b == nil {
			continue
		}
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
		if b == nil {
			continue
		}
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
	pcnt := 1
	ccnt := 1
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
	noData.Release()
	wg_p.Wait()


	for i := 0; i < ccnt; i++ {
		c_args[i].close.Close()
	}

	wg_c.Wait()
	hasData.Release()

	for i := 0; i < pcnt; i++ {
		require.True(t, p_args[i].close.IsClosed())
	}
	for i := 0; i < ccnt; i++ {
		require.True(t, c_args[i].close.IsClosed())
	}
	require.True(t, noData.close.IsClosed())
	require.True(t, hasData.close.IsClosed())
	fmt.Printf("%d %d\n",noData.GetBlockCount(),hasData.GetBlockCount())
	require.True(t, noData.GetBlockCount() + hasData.GetBlockCount() == blockcnt)
}

func Test_loadDataFromFileRoutine(t *testing.T) {
	f := "test/loadfile.csv"
	dataFile,err := os.Open(f)
	if err != nil {
		logutil.Errorf("open file failed. err:%v",err)
		return
	}
	defer func() {
		err := dataFile.Close()
		if err != nil{
			logutil.Errorf("close file failed. err:%v",err)
		}
	}()

	//create blocks for loading data
	blockCnt := 10
	blockSize := 3
	//block has no data, empty block
	blockPoolWithoutData := NewBlockPool(blockCnt,blockSize)
	//block has data loaded from file
	blockChannel := make(chan *Block,blockCnt)
	close := &CloseFlag{}

	go func() {
		err := loadDataFromFileRoutine(close,dataFile,blockPoolWithoutData,blockChannel)
		if err != nil {
			logutil.Errorf("load data filed.err:%v",err)
		}
	}()

	//============================
	var data []byte = nil

	for blk := range blockChannel {
		if blk == nil {
			break
		}
		//fmt.Printf("get block %d\n",blk.id)
		for {
			ok,d := blk.Get()
			if !ok {
				break
			}
			data = append(data,d)
			//if d == '\n' {
			//	fmt.Println()
			//}else{
			//	fmt.Printf("%v ",d)
			//}
		}
		//fmt.Printf("\n")
		blockPoolWithoutData.PutBlock(blk)
	}

	inData,err :=os.ReadFile(f)
	if err != nil {
		return
	}

	require.True(t, bytes.Compare(data,inData) == 0)
}

func Test_processBlock_startingby(t *testing.T) {

}

func gen_blks(blockSize int,s ...string) []*Block {
	var b []*Block
	for _, ss := range s {
		blk := NewBlock(0,blockSize)
		if !blk.PutBytes([]byte(ss)) {
			panic(fmt.Errorf("putbytes failed"))
		}
		b = append(b,blk)
	}
	return b
}

func gen_blks2(blockSize int,s ...[]byte) []*Block {
	var b []*Block
	for _, ss := range s {
		blk := NewBlock(0,blockSize)
		if !blk.PutBytes(ss) {
			panic(fmt.Errorf("putbytes failed"))
		}
		b = append(b,blk)
	}
	return b
}

func Test_findStringAmongBlocks(t *testing.T) {
	type args struct {
		handler    *ParseLineHandler
		blk        *Block
		sub        string
		nextStatus LINE_STATE
		want_ridx int
		want_widx int
		want_blk []*Block
	}
	s1 := "abcdefghijklmnopq"
	s2 := "rstuvwxyz"
	s3 := "zxyttsrqpo"
	blockSize := 20

	t1_blks := gen_blks(blockSize,s1)
	t2_blks := gen_blks(blockSize,s1)
	t3_blks := gen_blks(blockSize,s1)
	t4_blks := gen_blks(blockSize,s1,s2)
	t5_blks := gen_blks(blockSize,s1,s2,s3)
	t6_blks := gen_blks(blockSize,s1,s2,s3)
	t7_blks := gen_blks(blockSize,s1,s2,s3)
	t8_blks := gen_blks(blockSize,s1,s2,s3)
	t9_blks := gen_blks(blockSize,s1)

	tests := []struct {
		name string
		args args
	}{
		{
			"t1",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: nil,
					fields:        nil,
					lines:         nil,
				},
				blk:t1_blks[0],
				sub:"cde",
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: 5,
				want_widx: len(s1),
				want_blk: t1_blks,
			},
		},
		{
			"t2",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: nil,
					fields:        nil,
					lines:         nil,
				},
				blk:t2_blks[0],
				sub:"opq",
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: len(s1),
				want_widx: len(s1),
				want_blk: nil,
			},
		},
		{
			"t3",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: nil,
					fields:        nil,
					lines:         nil,
				},
				blk:t3_blks[0],
				sub:"opqr",
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: len(s1) - len("opgr")+1,
				want_widx: len(s1),
				want_blk: t3_blks,
			},
		},
		{
			"t4",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: []*Block{
						t4_blks[0],
					},
					fields:        nil,
					lines:         nil,
				},
				blk:t4_blks[1],
				sub:"opqr",
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: 1,
				want_widx: len(s2),
				want_blk: []*Block{
					t4_blks[1],
				},
			},
		},
		{
			"t5",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: []*Block{
						t5_blks[0],
						t5_blks[1],
					},
					fields:        nil,
					lines:         nil,
				},
				blk:t5_blks[2],
				sub:"opqrstuvwxyzzxyttsr",
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: 7,
				want_widx: len(s3),
				want_blk: []*Block{
					t5_blks[2],
				},
			},
		},
		{
			"t6",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: []*Block{
						t6_blks[0],
						t6_blks[1],
					},
					fields:        nil,
					lines:         nil,
				},
				blk:t6_blks[2],
				sub:"opqrstuvwxyzzxyttsk",
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: 1,
				want_widx: len(s2),
				want_blk: []*Block{
					t6_blks[1],
					t6_blks[2],
				},
			},
		},
		{
			"t7",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: []*Block{
						t7_blks[0],
						t7_blks[1],
					},
					fields:        nil,
					lines:         nil,
				},
				blk:t7_blks[2],
				sub:"zz",
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: 1,
				want_widx: len(s3),
				want_blk: []*Block{
					t7_blks[2],
				},
			},
		},
		{
			"t8",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: []*Block{
						t8_blks[0],
						t8_blks[1],
					},
					fields:        nil,
					lines:         nil,
				},
				blk:t8_blks[2],
				sub:s1+s2+s3,
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: 0,
				want_widx: 0,
				want_blk: []*Block{
				},
			},
		},
		{
			"t9",
			args{
				handler:&ParseLineHandler{
					status:        0,
					partialBlocks: nil,
					fields:        nil,
					lines:         nil,
				},
				blk:t9_blks[0],
				sub:"",
				nextStatus: LINE_STATE_PROCESS_PREFIX,
				want_ridx: 0,
				want_widx: len(s1),
				want_blk: []*Block{
					t9_blks[0],
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			findStringAmongBlocks(tt.args.handler,tt.args.blk,tt.args.sub,tt.args.nextStatus)
			require.True(t, len(tt.args.want_blk) == len(tt.args.handler.partialBlocks))
			for i := 0; i < len(tt.args.want_blk); i++ {
				require.True(t, tt.args.want_blk[i] == tt.args.handler.partialBlocks[i])
				if i == 0 {
					blk := tt.args.want_blk[0]
					require.True(t, blk.GetReadIdx() == tt.args.want_ridx && blk.GetWriteIdx() == tt.args.want_widx)
				}
			}
		})
	}
}

func Test_splitFields(t *testing.T) {
	type args struct {
		handler *ParseLineHandler
		blk     *Block
	}

	s1 := `1234,"abc\"def","abc",`
	s2 := `1234,"abc\"def","abc"`
	s3 := `1234,"abc\"def",abc`
	s4 := `1234,"abc\"def",abc`
	s5 := `1234,"abc\"def",abc,`
	s6 := `12,,34,,56`
	//s7 := `12,,34,,56`

	blockSize := 10000

	t1_blks := gen_blks2(blockSize,append(codec.String2Bytes(s1),10))
	t2_blks := gen_blks2(blockSize,append(codec.String2Bytes(s2),10))
	t3_blks := gen_blks(blockSize,s3)
	t4_blks := gen_blks2(blockSize,append(codec.String2Bytes(s4),10))
	t5_blks := gen_blks2(blockSize,append(codec.String2Bytes(s5),10))

	t6_slice := append(codec.String2Bytes(s1),10)
	t6_slice = append(t6_slice,append(codec.String2Bytes(s2),10)...)
	t6_slice = append(t6_slice,append(codec.String2Bytes(s4),10)...)
	t6_slice = append(t6_slice,append(codec.String2Bytes(s5),10)...)

	t6_blks := gen_blks2(blockSize,t6_slice)

	t7_slice := append(codec.String2Bytes(s1),10)
	t7_slice = append(t7_slice,append(codec.String2Bytes(s2),10)...)
	t7_slice = append(t7_slice,append(codec.String2Bytes(s4),10)...)
	t7_slice = append(t7_slice,append(codec.String2Bytes(s5))...)

	t7_blks := gen_blks2(blockSize,t7_slice)

	t8_blks := gen_blks2(blockSize,[]byte{10})

	t9_blks := gen_blks2(blockSize,append(codec.String2Bytes(s6),10))

	tests := []struct {
		name  string
		args  args
		want  [][][]byte
		want1 [][]byte
		want2 []byte
	}{
		{"t1",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
				},
				blk:     t1_blks[0],
			},
		[][][]byte{
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
			},
		nil,
		nil,
		},
		{"t2",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
				},
				blk:     t2_blks[0],
			},
			[][][]byte{
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
			},
			nil,
			nil,
		},
		{"t3",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
				},
				blk:     t3_blks[0],
			},
			nil,
			[][]byte{
				[]byte("1234"),
				[]byte("abc\"def"),
			},
			[]byte("abc"),
		},
		{"t4",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
				},
				blk:     t4_blks[0],
			},
			[][][]byte{
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
			},
			nil,
			nil,
		},
		{"t5",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
				},
				blk:     t5_blks[0],
			},
			[][][]byte{
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
			},
			nil,
			nil,
		},
		{"t6",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
				},
				blk:     t6_blks[0],
			},
			[][][]byte{
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
			},
			nil,
			nil,
		},
		{"t7",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
				},
				blk:     t7_blks[0],
			},
			[][][]byte{
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
			},
			[][]byte{
				[]byte("1234"),
				[]byte("abc\"def"),
				[]byte("abc"),
			},
			nil,
		},
		{"t8",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
					lineArray: nil,
					fieldsArray:[][]byte{
						[]byte("1234"),
						[]byte("abc\"def"),
					},
					fieldData:[]byte("abc"),
				},
				blk:     t8_blks[0],
			},
			[][][]byte{
				[][]byte{
					[]byte("1234"),
					[]byte("abc\"def"),
					[]byte("abc"),
				},
			},
			nil,
			nil,
		},
		{"t9",
			args{
				handler: &ParseLineHandler{
					status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
					partialBlocks: nil,
					fields:        &tree.Fields{
						Terminated: ",",
						Optionally: true,
						EnclosedBy: '"',
						EscapedBy:  '\\',
					},
					lines:         &tree.Lines{
						StartingBy:   "",
						TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
					},
					lineArray: nil,
					fieldsArray:nil,
					fieldData:nil,
				},
				blk:     t9_blks[0],
			},
			[][][]byte{
				[][]byte{
					[]byte("12"),
					[]byte{},
					[]byte("34"),
					[]byte{},
					[]byte("56"),
				},
			},
			nil,
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1,got2 := splitFields(tt.args.handler, tt.args.blk)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitFields() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("splitFields() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("splitFields() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func Test_readTextFile(t *testing.T) {
	data,err := os.ReadFile("test/loadfile.csv")
	require.NoError(t, err)
	fmt.Printf("%v\n",data)
}

func loadDataFromFile(t *testing.T,f string)[]byte{
	data,err := os.ReadFile(f)
	require.NoError(t, err)
	return data
}

func Test_splitFields1(t *testing.T) {
	type args struct {
		handler *ParseLineHandler
		blk     *Block
	}
	blocksize := 10000
	f1 := "test/loadfile.csv" //has a '\n' at last
	f1_data := loadDataFromFile(t,f1)
	t1_blk := gen_blks2(blocksize,f1_data)

	f2 := "test/loadfile2.csv" //does not have a '\n' at last
	f2_data := loadDataFromFile(t,f2)
	t2_blk := gen_blks2(blocksize,f2_data)

	f3 := "test/loadfile3"
	t3_blk := gen_blks2(blocksize,loadDataFromFile(t,f3))

	f4 := "test/loadfile4"
	t4_blk := gen_blks2(blocksize,loadDataFromFile(t,f4))

	tests := []struct {
		name  string
		args  args
		want  [][][]byte
		want1 [][]byte
		want2 []byte
	}{
		{"t1",args{
			handler: &ParseLineHandler{
				status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
				partialBlocks: nil,
				fields:        &tree.Fields{
					Terminated: ",",
					Optionally: true,
					EnclosedBy: '"',
					EscapedBy:  '\\',
				},
				lines:         &tree.Lines{
					StartingBy:   "",
					TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
				},
				lineArray:     nil,
				fieldsArray:   nil,
				fieldData:     nil,
			},
			blk:     t1_blk[0],
		},
		//1,2,3,4,5,6,abc,1.04,10
		[][][]byte{
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
				[]byte("10"),
			},
		},nil,nil},
		{"t2",args{
			handler: &ParseLineHandler{
				status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
				partialBlocks: nil,
				fields:        &tree.Fields{
					Terminated: ",",
					Optionally: true,
					EnclosedBy: '"',
					EscapedBy:  '\\',
				},
				lines:         &tree.Lines{
					StartingBy:   "",
					TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
				},
				lineArray:     nil,
				fieldsArray:   nil,
				fieldData:     nil,
			},
			blk:     t2_blk[0],
		},
			//1,2,3,4,5,6,abc,1.04,10
			[][][]byte{
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
				[][]byte{
					[]byte("1"),
					[]byte("2"),
					[]byte("3"),
					[]byte("4"),
					[]byte("5"),
					[]byte("6"),
					[]byte("abc"),
					[]byte("1.04"),
					[]byte("10"),
				},
			},
			[][]byte{
				[]byte("1"),
				[]byte("2"),
				[]byte("3"),
				[]byte("4"),
				[]byte("5"),
				[]byte("6"),
				[]byte("abc"),
				[]byte("1.04"),
			},
			[]byte("10"),
		},
		{"t3",args{
			handler: &ParseLineHandler{
				status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
				partialBlocks: nil,
				fields:        &tree.Fields{
					Terminated: "|",
					Optionally: true,
					EnclosedBy: '"',
					EscapedBy:  '\\',
				},
				lines:         &tree.Lines{
					StartingBy:   "",
					TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
				},
				lineArray:     nil,
				fieldsArray:   nil,
				fieldData:     nil,
			},
			blk:     t3_blk[0],
		},
			[][][]byte{
				[][]byte{
					[]byte("1"),
					[]byte("Supplier#000000001"),
					[]byte("sdrGnXCDRcfriBvY0KL,i"),
					[]byte("PERU     0"),
					[]byte("PERU"),
					[]byte("AMERICA"),
					[]byte("27-989-741-2988"),
				},
				[][]byte{
					[]byte("2"),
					[]byte("Supplier#000000002"),
					[]byte("TRMhVHz3XiFu"),
					[]byte("ETHIOPIA 1"),
					[]byte("ETHIOPIA"),
					[]byte("AFRICA"),
					[]byte("15-768-687-3665"),
				},
				[][]byte{
					[]byte("3"),
					[]byte("Supplier#000000003"),
					[]byte("BZ0kXcHUcHjx62L7CjZS"),
					[]byte("ARGENTINA7"),
					[]byte("ARGENTINA"),
					[]byte("AMERICA"),
					[]byte("11-719-748-3364"),
				},
			},nil,nil},
		{"t4",args{
			handler: &ParseLineHandler{
				status:        LINE_STATE_PROCESS_FIELD_FIRST_BYTE,
				partialBlocks: nil,
				fields:        &tree.Fields{
					Terminated: ",",
					Optionally: true,
					EnclosedBy: '"',
					EscapedBy:  '\\',
				},
				lines:         &tree.Lines{
					StartingBy:   "",
					TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
				},
				lineArray:     nil,
				fieldsArray:   nil,
				fieldData:     nil,
			},
			blk:     t4_blk[0],
		},
			[][][]byte{
				[][]byte{
					[]byte("B00001"),
					[]byte("LONDON TOWNCARS INC"),
					[]byte(""),
					[]byte("other"),
				},
				[][]byte{
					[]byte("B00013"),
					[]byte("LOVE CORPORATE CAR INC"),
					[]byte(""),
					[]byte("other"),
				},
				[][]byte{
					[]byte("B00008"),
					[]byte("T-D MAINTENANCE CORP"),
					[]byte("FOUR ONES CAR SERVICE"),
					[]byte("other"),
				},
			},nil,nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2 := splitFields(tt.args.handler, tt.args.blk)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitFields() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("splitFields() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("splitFields() got2 = %v, want %v", got2, tt.want2)
			}
		})
	}
}

func loadAndProcess(t *testing.T,load *tree.Load,packline func([][][]byte)) {
	/*
		step1 : read block from file
	*/
	dataFile,err := os.Open(load.File)
	if err != nil {
		logutil.Errorf("open file failed. err:%v",err)
		return
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

	handler := &ParseLineHandler{
		status:       LINE_STATE_PROCESS_PREFIX,
		partialBlocks: nil,
		fields:       load.Fields,
		lines:        load.Lines,
		packLine: packline,
	}

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
		//TODO:
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
	}
}

func Test_loadAndProcess(t *testing.T) {
	/*
	//ssb
	date.tbl
	customer.tbl
	part.tbl
	supplier.tbl
	lineorder.tbl
	 */
	f1 := "../../../mo-test/lineorder.tbl"
	t1 := &tree.Load{
		Local:             false,
		File:              f1,
		DuplicateHandling: nil,
		Table:             nil,
		Fields:            &tree.Fields{
			Terminated: "|",
			Optionally: true,
			EnclosedBy: '"',
			EscapedBy:  '\\',
		},
		Lines:             &tree.Lines{
			StartingBy:   "",
			TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
		},
		IgnoredLines:      0,
		ColumnList:        nil,
		Assignments:       nil,
	}

	t1_packline := func(lineArray [][][]byte) {
		for _, line := range lineArray {
			var oline []byte = nil
			for _,field := range line{
				oline = append(oline,field...)
				oline = append(oline,byte('|'))
			}
			fmt.Printf("%s\n",codec.Bytes2String(oline))
		}
	}

	loadAndProcess(t,t1,t1_packline)
}

func Test_loadAndProcess2(t *testing.T) {
	/*
		//taxi
	central_park_weather.csv -- further check
	fhv_bases.csv
	taxi_head_10-2
	*/
	f1 := "../../../mo-test/taxi_head_10-2"
	t1 := &tree.Load{
		Local:             false,
		File:              f1,
		DuplicateHandling: nil,
		Table:             nil,
		Fields:            &tree.Fields{
			Terminated: ",",
			Optionally: true,
			EnclosedBy: '"',
			EscapedBy:  '\\',
		},
		Lines:             &tree.Lines{
			StartingBy:   "",
			TerminatedBy: "\n",//careful. \\n -> "\n" , \n -> '\n'
		},
		IgnoredLines:      0,
		ColumnList:        nil,
		Assignments:       nil,
	}

	lastFieldAppend := true
	t1_packline := func(lineArray [][][]byte) {
		for _, line := range lineArray {
			var oline []byte = nil
			for i,field := range line{
				oline = append(oline,field...)
				if lastFieldAppend == true {
					oline = append(oline,byte(','))
				}else{
					if i < len(line) - 1 {
						oline = append(oline,byte(','))
					}
				}

			}
			fmt.Printf("%s\n",codec.Bytes2String(oline))
		}
	}

	loadAndProcess(t,t1,t1_packline)
}