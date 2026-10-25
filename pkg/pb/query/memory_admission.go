// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
package query

// MemoryAdmissionProtocolVersion includes complete producer and expression
// capacity contracts plus durable pipeline/receiver and preallocated Abort metadata.
// Only an exact match may admit a controlled graph; older and unknown future
// contracts are not interchangeable capacity proofs.
const MemoryAdmissionProtocolVersion uint32 = 3
