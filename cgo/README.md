MatrixOne CGO Kernel
===============================

This directory contains cgo source code for MO.   Running
make should produce two files to be used by go code.
On go side, go will `include "mo.h"` and `-lmo`.   
```
mo.h
libmo.a
```

`mo.h` should be pristine, meaning it only contains C function
prototype used by go.  The only datatypes that can be passed 
between go and c code are int and float/double and pointer.   
Always explicitly specify int size such as `int32_t`, `uint64_t`.
Do not use `int`, `long`, etc.

Implementation Notes
--------------------------------

1. Pure C.
2. Use memory passed from go.  Try not allocate memory in C code.
3. Only depends on libc and libm.
4. If 3rd party lib is absolutely necessary, import source code 
   and build from source. If 3rd party lib is C++, wrap it completely in C.
