/**
 * The N|Solid API of nsolid-bun, NodeSource's build of Bun. `require("nsolid")`
 * is an alias of `bun:nsolid`, as in N|Solid for Node.js.
 *
 * The agent that reports to the N|Solid Console starts when `NSOLID_GRPC` or
 * `NSOLID_SAAS` is set.
 *
 * @example
 * ```ts
 * import nsolid from "nsolid";
 *
 * console.log(nsolid.id); // "2f7c…", 40 hexadecimal digits
 * ```
 */
declare module "bun:nsolid" {
  const nsolid: {
    /**
     * The id the N|Solid Console knows this process by: 40 hexadecimal
     * digits. One per process, the same on every thread, whether or not the
     * agent is connected.
     */
    readonly id: string;
  };
  export default nsolid;
}

declare module "nsolid" {
  export { default } from "bun:nsolid";
}
