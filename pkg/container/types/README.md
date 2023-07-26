## Embedding/FloatVector

### Registering FloatVector to Vector Types.

There are a couple of strategies of doing this.

1. Using Width with Float[]

Problem: Dealing with TypeSize()
```go
func DecodeFixedCol[T types.FixedSizeT](v *Vector) []T {
	sz := int(v.typ.TypeSize()) // TypeSize is not defined for FloatVector

	if cap(v.data) >= sz {
		return unsafe.Slice((*T)(unsafe.Pointer(&v.data[0])), cap(v.data)/sz)
	}
	return nil
}
```
Currently TypeSize() is solved using 
```go
func (t Type) TypeSize() int {
	if t.Oid == T_float32vec {
		return int(t.Width * t.Size)
	}
	return int(t.Size)
}
```

2. Using Width without creating a new Oid
Problem: 
Each of the float switch case may now have 2 condition: 1 for regular float, 2 for float vector.
To simplify this, we can define a new Oid and have a new switch case for that Oid.

3. Using Varlena
Problem: Right now area saves bytes. We may encounter serialization/deserialization overhead.

### Adding support for Create Syntax in Parser

### Adding support for INSERT/SELECT syntax in Parser and Execution Engine.

### Implement unary operators like minus, abs etc.


### Implement binary operator

### Implement aggregate operator for `Transpose`.

### Start with Indexing Task.
- Similar to our current compaction task
- Will be done in CN node.
- 