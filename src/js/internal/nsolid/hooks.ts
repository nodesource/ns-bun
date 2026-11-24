// Where Bun's own modules report to the N|Solid agent's tracing and metrics (internal/nsolid/tracing.ts). The agent
// sets these on each thread it watches; elsewhere they stay undefined, and a module only checks one property.

export default {
  /**
   * node:http's server, around the `'request'` event: a scope in which the request's span is the active one,
   * disposed of once the listeners have run.
   */
  httpServerRequest: undefined as ((request: unknown, response: unknown) => Disposable) | undefined,
  /** node:dns: the resolver its queries go through, which traces them. */
  dnsResolver: undefined as (<T extends object>(resolver: T) => T) | undefined,
};
