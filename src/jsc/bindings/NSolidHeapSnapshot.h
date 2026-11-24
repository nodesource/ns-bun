#pragma once

#include "root.h"

namespace Bun {

// A heap snapshot in V8's format (.heapsnapshot), the one the N|Solid Console
// reads, of the heap of `vm`, taken on its thread after a full collection.
// With `redact`, no string's text is in it: string nodes are named
// "(redacted)", as N|Solid's are.
// The JSON comes in pieces of about `chunkBytes`, never as one string.
Vector<Vector<uint8_t>> takeNSolidHeapSnapshot(JSC::VM&, bool redact, size_t chunkBytes);

// What taking a snapshot of `vm`'s heap would use, in bytes, at most.
uint64_t nsolidHeapSnapshotCost(JSC::VM&, bool redact);

// The memory the process can still use, in bytes: the system's available
// memory, or what is left under the process's cgroup limit, if less.
uint64_t nsolidAvailableMemory();

}
