// Installs the LLVM that Bun's CI images pin (scripts/build/ci-images/spec.ts)
// from LLVM's GitHub release, with the Ubuntu 22.04 ICU its lld is linked
// against, checking both downloads' sha256. For machines that can't use
// apt.llvm.org: nsolid-bun's CI and restricted cloud sessions.
//
//   bun scripts/nsolid/install-llvm.ts [directory]   (default: /opt/llvm)
//
// Then put <directory>/bin first on PATH.

import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { copyFileSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pins } from "../build/ci-images/spec.ts";

const directory = process.argv[2] ?? "/opt/llvm";
const arch = process.arch === "arm64" ? "aarch64" : "x64";
if (process.platform !== "linux") throw new Error("install-llvm.ts is for Linux");

async function download(url: string, sha256: string): Promise<Uint8Array> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`${url}: HTTP ${response.status}`);
  const bytes = new Uint8Array(await response.arrayBuffer());
  const actual = createHash("sha256").update(bytes).digest("hex");
  if (actual !== sha256) throw new Error(`${url}: sha256 ${actual}, expected ${sha256}`);
  return bytes;
}

function run(cmd: string, args: string[]) {
  const result = spawnSync(cmd, args, { stdio: "inherit" });
  if (result.status !== 0) throw new Error(`${cmd} ${args.join(" ")} exited with ${result.status}`);
}

const work = mkdtempSync(join(tmpdir(), "install-llvm-"));
try {
  const { version, sha256, icu } = pins.llvm;
  const asset = `LLVM-${version}-Linux-${arch === "x64" ? "X64" : "ARM64"}.tar.xz`;
  const tarball = join(work, asset);
  console.log(`LLVM ${version}`);
  writeFileSync(
    tarball,
    await download(`https://github.com/llvm/llvm-project/releases/download/llvmorg-${version}/${asset}`, sha256[arch]),
  );
  rmSync(directory, { recursive: true, force: true });
  mkdirSync(directory, { recursive: true });
  run("tar", ["-xJf", tarball, "-C", directory, "--strip-components=1"]);

  const debArch = arch === "x64" ? "amd64" : "arm64";
  const mirror = arch === "x64" ? "http://archive.ubuntu.com/ubuntu" : "http://ports.ubuntu.com/ubuntu-ports";
  const deb = join(work, `${icu.name}.deb`);
  console.log(`${icu.name} ${icu.version}`);
  writeFileSync(
    deb,
    await download(`${mirror}/pool/main/${icu.directory}/${icu.name}_${icu.version}_${debArch}.deb`, icu.sha256[arch]),
  );
  const extracted = join(work, "icu");
  run("dpkg-deb", ["-x", deb, extracted]);
  const libDir = join(extracted, "usr", "lib", `${arch === "x64" ? "x86_64" : "aarch64"}-linux-gnu`);
  for (const file of readdirSync(libDir)) {
    if (file.startsWith("libicu")) copyFileSync(join(libDir, file), join(directory, "lib", file));
  }
  run(join(directory, "bin", "clang"), ["--version"]);
} finally {
  rmSync(work, { recursive: true, force: true });
}
