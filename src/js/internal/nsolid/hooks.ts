// Where Bun's own modules report to the N|Solid agent's tracing and metrics (internal/nsolid/tracing.ts). The agent
// sets these on each thread it watches; elsewhere they stay undefined, and a module only checks one property.

export default {
  /**
   * node:http's server, around the `'request'` event: a scope in which the request's span is the active one,
   * disposed of once the listeners have run. `headersOf` gives the request's headers without reading `req.headers`.
   */
  httpServerRequest: undefined as
    | ((
        request: unknown,
        response: unknown,
        headersOf: (request: unknown) => Record<string, string> | undefined,
      ) => Disposable)
    | undefined,
  /**
   * node:http's server, as a response emits `'finish'` and as it emits `'close'`. `this` of the emit may be any
   * object, so whether its headers were sent is asked only of a response the agent saw begin.
   */
  httpServerResponseEnd: undefined as
    ((response: unknown, closed: boolean, headersSent: (response: unknown) => boolean) => void) | undefined,
  /** node:http's client, once a response's body has ended or the request's socket has closed. */
  httpClientResponseEnd: undefined as ((request: unknown) => void) | undefined,
  /** node:dns: the resolver its queries go through, which traces them. */
  dnsResolver: undefined as (<T extends object>(resolver: T) => T) | undefined,
};
