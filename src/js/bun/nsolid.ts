// The public N|Solid API: `require("nsolid")`, which is an alias of
// `bun:nsolid`, as in N|Solid for Node.js.

const agentId = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidAgentId", 0);

export default {
  // The id the N|Solid Console knows this process by. One per process, the
  // same on every thread, whether or not the agent is connected.
  id: agentId() as string,
};
