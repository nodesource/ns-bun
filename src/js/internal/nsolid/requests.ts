// The agent's requests to the application's threads: a thread reads what only
// it can (its heap, its loaded modules) and answers on the agent's thread. A
// thread that is gone, or busy for longer than `timeoutMs`, answers undefined.

const onReply = $newCppFunction("NSolidAgent.cpp", "jsFunctionNSolidOnReply", 1);

let nextRequestId = 1;
const pending = new Map<number, (value: unknown) => void>();
onReply((requestId: number, value: unknown) => {
  const resolve = pending.get(requestId);
  if (resolve) {
    pending.delete(requestId);
    resolve(value);
  }
});

function ask<T>(post: (contextId: number, requestId: number) => boolean, contextId: number, timeoutMs: number) {
  const requestId = nextRequestId++;
  if (!post(contextId, requestId)) return Promise.resolve(undefined);
  const { promise, resolve } = Promise.withResolvers<T | undefined>();
  const timer = setTimeout(() => {
    if (pending.delete(requestId)) resolve(undefined);
  }, timeoutMs);
  pending.set(requestId, value => {
    clearTimeout(timer);
    resolve(value as T);
  });
  return promise;
}

export default { ask };
