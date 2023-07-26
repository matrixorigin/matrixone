# Embedding

### Registering FloatVector to Vector Types.

There are a couple of strategies of doing this.

1. Using Varlena  (suggested by Feng)
2. Using Width with Float[] (suggested by MoChen) - Good.
```go
func DecodeFixedCol[T types.FixedSizeT](v *Vector) []T {
	sz := int(v.typ.TypeSize()) // What to do for TypeSize()?

	//if cap(v.data)%sz != 0 {
	//	panic(moerr.NewInternalErrorNoCtx("decode slice that is not a multiple of element size"))
	//}

	if cap(v.data) >= sz {
		return unsafe.Slice((*T)(unsafe.Pointer(&v.data[0])), cap(v.data)/sz)
	}
	return nil
}
```

3. Using Width without creating a new Oid (suggested by Aungr) - Complex
