#pragma once

#include "root.h"

namespace Bun {

// `nsolid.id`: the id the N|Solid Console knows this process by.
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidAgentId);

// For the agent: the JavaScript threads of the process (the main thread and
// each running worker), as `[{ contextId, threadId, name }]`.
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidThreads);

// For the agent: asks one thread (contextId, requestId) for its heap figures,
// or for the modules it loaded. They are read on that thread and handed, with
// the request id, to the function given to `jsFunctionNSolidOnReply`, on the
// agent's. Returns false if the thread is gone.
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidOnReply);
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidRequestThreadHeap);
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidRequestLoadedModules);

// For the agent: the function to call with the process's exit
// (code, signal, error message, error stack), and the call that says the exit
// event was sent, which lets the exiting thread go on.
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidOnExit);
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidExitSent);

// For `nsolid`: the agent's configuration as it publishes it (JSON), and
// `nsolid.start(config)`, which the agent listens for.
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidPublishConfig);
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidSharedConfig);
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidOnStart);
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidStart);

// For `nsolid`: when the process started (ms since the epoch), and the
// modules the calling thread loaded.
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidProcessStart);
JSC_DECLARE_HOST_FUNCTION(jsFunctionNSolidLoadedModules);

}
