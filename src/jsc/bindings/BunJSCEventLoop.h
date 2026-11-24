#pragma once

#include "root.h"

namespace Bun {

// For perf_hooks: [loopStart, idleTime] of this thread's event loop, in
// milliseconds on performance.now()'s clock. loopStart is -1 until the loop
// first polls.
JSC_DECLARE_HOST_FUNCTION(jsFunctionEventLoopTiming);

}
