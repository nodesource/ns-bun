// The application's packages, listed the way N|Solid lists them (lib/nsolid.js
// `genPackageList`, lib/internal/nsolid_module.js): every package in the
// node_modules directories the main script can require from, and the packages
// their dependencies resolve to, each dependency then named by its path
// relative to the package. A package is `required` once the main thread has
// loaded a module of it.
//
// Two differences, for bun install's isolated linker: a package's directory
// may be a symlink, and dependencies are looked up from the package's real
// directory, where its own dependencies are linked beside it.

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

function readJson(file: string): Record<string, any> | undefined {
  try {
    const value = JSON.parse(fs.readFileSync(file, "utf8"));
    return value && typeof value === "object" ? value : undefined;
  } catch {
    return undefined;
  }
}

function realpath(dir: string): string {
  try {
    return fs.realpathSync(dir);
  } catch {
    return dir;
  }
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

  add(dir: string, json: Record<string, any>, required = false): { info: Package; added: boolean } {
    const real = realpath(dir);
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
  scan() {
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
    for (const modulePath of new Set(modulePaths)) this.scanDirectory(modulePath, false);
  }

  scanDirectory(dir: string, inScope: boolean) {
    let entries;
    try {
      entries = fs.readdirSync(dir, { withFileTypes: true });
    } catch {
      return;
    }
    for (const entry of entries) {
      if (entry.name[0] === ".") continue;
      const entryPath = path.resolve(dir, entry.name);
      if (!entry.isDirectory()) {
        if (!entry.isSymbolicLink()) continue;
        try {
          if (!fs.statSync(entryPath).isDirectory()) continue;
        } catch {
          continue;
        }
      }
      if (!inScope && entry.name[0] === "@") {
        this.scanDirectory(entryPath, true);
        continue;
      }
      const json = readJson(path.resolve(entryPath, "package.json"));
      if (!json) continue;
      const { info, added } = this.add(entryPath, json);
      if (added) this.collect(info);
    }
  }

  // `recurseCollect`: each dependency that resolves is listed too, and named
  // by its path relative to the package.
  collect(root: Package) {
    const queue = [root];
    while (queue.length) {
      const info = queue.shift()!;
      const from = realpath(info.path);
      for (let i = 0; i < info.dependencies.length; i++) {
        const found = this.findModule(from, info.dependencies[i]);
        if (!found) continue;
        info.dependencies[i] = path.relative(info.path, found.dir);
        const { info: dependency, added } = this.add(found.dir, found.json);
        if (added) queue.push(dependency);
      }
    }
  }

  // `findModule` and `getModulePath`: the nearest node_modules/<name> with a
  // package.json, going up from `dir` and passing over node_modules
  // directories themselves.
  findModule(dir: string, name: string): { dir: string; json: Record<string, any> } | undefined {
    for (let last; last !== dir;) {
      if (path.basename(dir) !== "node_modules") {
        const candidate = path.resolve(dir, "node_modules", name);
        const json = fs.existsSync(candidate) ? readJson(path.resolve(candidate, "package.json")) : undefined;
        if (json) return { dir: candidate, json };
      }
      last = dir;
      dir = path.resolve(dir, "..");
    }
  }

  // The packages a thread loaded a module of: `modules` are the keys of its
  // module registry and require.cache.
  markRequiredFrom(modules: string[]) {
    const dirs = new Set<string>();
    for (const module of modules) {
      if (!path.isAbsolute(module)) continue;
      const dir = packageDirOf(module);
      if (dir) dirs.add(dir);
    }
    for (const dir of dirs) {
      const known = this.byRealPath.get(realpath(dir));
      if (known) {
        known.required = true;
        continue;
      }
      const json = readJson(path.resolve(dir, "package.json"));
      if (json) this.add(dir, json, true);
    }
  }

  packagesOf(loadedModules: string[]): Package[] {
    this.scan();
    this.markRequiredFrom(loadedModules);
    return this.list;
  }
}

export default { Inventory, packageDirOf };
