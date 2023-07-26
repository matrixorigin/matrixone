## Embedding/FloatVector

### Registering FloatVector to Vector Types.

There are a couple of strategies of doing this.

1. Using Varlena 
2. Using Width with Float[]

Problem: Dealing with TypeSize()
```go
func DecodeFixedCol[T types.FixedSizeT](v *Vector) []T {
	sz := int(v.typ.TypeSize()) // What to do for TypeSize()?

	if cap(v.data) >= sz {
		return unsafe.Slice((*T)(unsafe.Pointer(&v.data[0])), cap(v.data)/sz)
	}
	return nil
}
```

3. Using Width without creating a new Oid
Problem: 
Each of the float switch case may now have 2 condition. 1 for regular float, 2 for float vector.
To simplify this, we can define a new Oid and have a new switch case for that Oid.

### Adding support for Create Syntax in Parser


### Adding support for INSERT/SELECT syntax in Parser and Execution Engine.

### Implement unary operators like minus, abs etc.


### Implement binary operator

### Implement aggregate operator for `Transpose`.

### Start with Indexing Task.
- Similar to our current compaction task
- Will be done in CN node.
- 