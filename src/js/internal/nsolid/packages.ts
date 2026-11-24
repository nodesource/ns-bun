// The application's packages, listed the way N|Solid lists them (lib/nsolid.js
// `genPackageList`, lib/internal/nsolid_module.js): every package in the
// node_modules directories the main script can require from, and the packages
// their dependencies resolve to, each dependency then named by its path
// relative to the package. A package is `required` once the thread asked
// about has loaded a module of it.
//
// Two differences, for bun install's isolated linker: a package's directory
// may be a symlink, and dependencies are looked up from the package's real
// directory, where its own dependencies are linked beside it.
//
// The work is written as steps that ask for file system reads, so that the
// agent can run it without holding up its thread (a large node_modules takes
// seconds), and `nsolid.packages()` can run it at once, as N|Solid's does.

const fs = require("node:fs");
const path = require("node:path");

export interface Package {
  path: string;
  name: string;
  version: string | undefined;
  main: string | undefined;
  dependencies: string[];
  required: boolean;
}

// What a step asks for. Each answers undefined when the read fails.
type Read =
  | { read: "directory"; path: string }
  | { read: "isDirectory"; path: string }
  | { read: "json"; path: string }
  | { read: "realpath"; path: string };

type Steps<T> = Generator<Read, T, any>;

function parseJson(text: string): Record<string, any> | undefined {
  const value = JSON.parse(text);
  return value && typeof value === "object" ? value : undefined;
}

const readAtOnce: Record<Read["read"], (path: string) => unknown> = {
  directory: path => fs.readdirSync(path, { withFileTypes: true }),
  isDirectory: path => fs.statSync(path).isDirectory(),
  json: path => parseJson(fs.readFileSync(path, "utf8")),
  realpath: path => fs.realpathSync(path),
};

const readLater: Record<Read["read"], (path: string) => Promise<unknown>> = {
  directory: path => fs.promises.readdir(path, { withFileTypes: true }),
  isDirectory: async path => (await fs.promises.stat(path)).isDirectory(),
  json: async path => parseJson(String(await fs.promises.readFile(path, "utf8"))),
  realpath: path => fs.promises.realpath(path),
};

function runAtOnce<T>(steps: Steps<T>): T {
  let step = steps.next();
  while (!step.done) {
    let value;
    try {
      value = readAtOnce[step.value.read](step.value.path);
    } catch {}
    step = steps.next(value);
  }
  return step.value;
}

async function runLater<T>(steps: Steps<T>): Promise<T> {
  let step = steps.next();
  while (!step.done) {
    let value;
    try {
      value = await readLater[step.value.read](step.value.path);
    } catch {}
    step = steps.next(value);
  }
  return step.value;
}

function packageInfo(dir: string, json: Record<string, any>, required: boolean): Package {
  const dependencies = new Set<string>();
  for (const field of ["dependencies", "optionalDependencies", "bundledDependencies"]) {
    if (json[field] && typeof json[field] === "object")
      for (const name of Object.keys(json[field])) dependencies.add(name);
  }
  return {
    path: dir,
    name: json.name || "undefined",
    version: typeof json.version === "string" ? json.version : undefined,
    main: typeof json.main === "string" ? json.main : undefined,
    dependencies: [...dependencies],
    required,
  };
}

// A module file's package directory: what follows its last node_modules
// segment, with the scope of a scoped package.
function packageDirOf(file: string): string | undefined {
  const parts = file.split(path.sep);
  const at = parts.lastIndexOf("node_modules");
  if (at === -1 || at + 1 >= parts.length - 1) return;
  const end = parts[at + 1].startsWith("@") ? at + 3 : at + 2;
  if (end > parts.length - 1) return;
  return parts.slice(0, end).join(path.sep);
}

class Inventory {
  list: Package[] = [];
  // Keyed by real directory: one package reached through a symlink and
  // through its real path is listed once.
  byRealPath = new Map<string, Package>();
  scanned = false;

  constructor(
    public mainPath: string,
    public scanEnabled: boolean,
    public globalPaths: string[],
  ) {}

  *realpath(dir: string): Steps<string> {
    return ((yield { read: "realpath", path: dir }) as string | undefined) ?? dir;
  }

  *add(dir: string, json: Record<string, any>, required = false): Steps<{ info: Package; added: boolean }> {
    const real = yield* this.realpath(dir);
    const known = this.byRealPath.get(real);
    if (known) {
      if (required) known.required = true;
      return { info: known, added: false };
    }
    const info = packageInfo(dir, json, required);
    this.byRealPath.set(real, info);
    this.list.push(info);
    return { info, added: true };
  }

  // `genPackageList`: the node_modules of the main script's directory and of
  // each directory above it, and the global ones with trackGlobalPackages.
  *scan(): Steps<void> {
    if (this.scanned || !this.scanEnabled) return;
    this.scanned = true;
    let dir: string;
    try {
      dir = this.mainPath ? path.dirname(this.mainPath) : process.cwd();
    } catch {
      return;
    }
    const modulePaths = [...this.globalPaths];
    for (let last; last !== dir; dir = path.resolve(dir, "..")) {
      last = dir;
      modulePaths.push(path.resolve(dir, "node_modules"));
    }
    for (const modulePath of new Set(modulePaths)) yield* this.scanDirectory(modulePath, false);
  }

  *scanDirectory(dir: string, inScope: boolean): Steps<void> {
    const entries = yield { read: "directory", path: dir };
    if (!entries) return;
    for (const entry of entries) {
      if (entry.name[0] === ".") continue;
      const entryPath = path.resolve(dir, entry.name);
      if (!entry.isDirectory() && !(entry.isSymbolicLink() && (yield { read: "isDirectory", path: entryPath }))) {
        continue;
      }
      if (!inScope && entry.name[0] === "@") {
        yield* this.scanDirectory(entryPath, true);
        continue;
      }
      const json = yield { read: "json", path: path.resolve(entryPath, "package.json") };
      if (!json) continue;
      const { info, added } = yield* this.add(entryPath, json);
      if (added) yield* this.collect(info);
    }
  }

  // `recurseCollect`: each dependency that resolves is listed too, and named
  // by its path relative to the package.
  *collect(root: Package): Steps<void> {
    const queue = [root];
    while (queue.length) {
      const info = queue.shift()!;
      const from = yield* this.realpath(info.path);
      for (let i = 0; i < info.dependencies.length; i++) {
        const found = yield* this.findModule(from, info.dependencies[i]);
        if (!found) continue;
        info.dependencies[i] = path.relative(info.path, found.dir);
        const { info: dependency, added } = yield* this.add(found.dir, found.json);
        if (added) queue.push(dependency);
      }
    }
  }

  // `findModule` and `getModulePath`: the nearest node_modules/<name> with a
  // package.json, going up from `dir` and passing over node_modules
  // directories themselves.
  *findModule(dir: string, name: string): Steps<{ dir: string; json: Record<string, any> } | undefined> {
    for (let last; last !== dir;) {
      if (path.basename(dir) !== "node_modules") {
        const candidate = path.resolve(dir, "node_modules", name);
        const json = yield { read: "json", path: path.resolve(candidate, "package.json") };
        if (json) return { dir: candidate, json };
      }
      last = dir;
      dir = path.resolve(dir, "..");
    }
  }

  // The packages a thread loaded a module of: `modules` are the keys of its
  // module registry and require.cache.
  *markRequired(modules: string[]): Steps<void> {
    const dirs = new Set<string>();
    for (const module of modules) {
      if (!path.isAbsolute(module)) continue;
      const dir = packageDirOf(module);
      if (dir) dirs.add(dir);
    }
    for (const dir of dirs) {
      const known = this.byRealPath.get(yield* this.realpath(dir));
      if (known) {
        known.required = true;
        continue;
      }
      const json = yield { read: "json", path: path.resolve(dir, "package.json") };
      if (json) yield* this.add(dir, json, true);
    }
  }

  *packages(loadedModules: string[]): Steps<Package[]> {
    yield* this.scan();
    yield* this.markRequired(loadedModules);
    return this.list;
  }

  // For the agent: without holding up its thread.
  packagesLater(loadedModules: string[]): Promise<Package[]> {
    return runLater(this.packages(loadedModules));
  }

  // For `nsolid.packages()`, at once.
  packagesAtOnce(loadedModules: string[]): Package[] {
    return runAtOnce(this.packages(loadedModules));
  }
}

export default { Inventory, packageDirOf };
