package objectio

import "fmt"

type IOEntryType = uint16
type IOEntryVersion = uint16

const (
	IOET_Empty   = 0
	IOET_ObjMeta = 1
	IOET_ColData = 2
	IOET_BF      = 3
	IOET_BlkObj  = 4
)

type IOEntryHeader struct {
	Type, Version uint16
}

func (h IOEntryHeader) String() string {
	return fmt.Sprintf("IOEntry[%d,%d]", h.Type, h.Version)
}

type IOEntry interface {
	MarshalBinary() ([]byte, error)
	UnmarshalBinary([]byte) error
}

type IOEncodeFunc = func(IOEntry) ([]byte, error)
type IODecodeFunc = func([]byte) (IOEntry, error)

type ioEntryCodec struct {
	// if encFn is nil, no need to encode
	encFn IOEncodeFunc

	// if decFn is nil, no need to decode
	decFn IODecodeFunc
}

func (codec ioEntryCodec) NoMarshal() bool {
	return codec.encFn == nil
}

func (codec ioEntryCodec) NoUnmarshal() bool {
	return codec.decFn == nil
}

var ioEntryCodecs = map[IOEntryHeader]ioEntryCodec{}

func RegisterIOEnrtyCodec(h IOEntryHeader, encFn IOEncodeFunc, decFn IODecodeFunc) {
	_, ok := ioEntryCodecs[h]
	if ok {
		panic(fmt.Sprintf("duplicate io entry codec found: %s", h.String()))
	}
	ioEntryCodecs[h] = ioEntryCodec{
		encFn: encFn,
		decFn: decFn,
	}
}

func GetIOEntryCodec(h IOEntryHeader) (codec ioEntryCodec) {
	var ok bool
	codec, ok = ioEntryCodecs[h]
	if !ok {
		panic(fmt.Sprintf("no codec found for: %s", h.String()))
	}
	return
}
