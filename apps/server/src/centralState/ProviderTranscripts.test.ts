// @effect-diagnostics nodeBuiltinImport:off - seeds provider transcripts with exact bytes and mtimes.
import * as NodeFS from "node:fs";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";

import * as Blobs from "./Blobs.ts";
import * as CentralObjectStore from "./CentralObjectStore.ts";
import { TRANSCRIPT_MAX_CHUNKS, type ClaudeSection } from "./Manifest.ts";
import * as ProviderTranscripts from "./ProviderTranscripts.ts";

const homeKey = "dev";
const transcript = "-workspace-app/0a0a0a0a-0000-4000-8000-000000000001.jsonl";
const toolResult = "-workspace-app/0a0a0a0a-0000-4000-8000-000000000001/tool-results/out.txt";
const empty: typeof ClaudeSection.Type = { files: {} };
const rollout = "2026/11/16/rollout-2026-11-16T10-00-00-0a0a0a0a-0000-4000-8000-000000000002.jsonl";
const archivedRollout = "rollout-2026-11-15T10-00-00-0a0a0a0a-0000-4000-8000-000000000003.jsonl";

/** Claude and Codex homes and a filesystem store whose blob uploads are counted. */
const setup = Effect.gen(function* () {
  const fileSystem = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fileSystem.makeTempDirectoryScoped({ prefix: "t3-claude-transcripts-" });
  const inner = Context.get(
    yield* Layer.build(CentralObjectStore.layerFilesystem(path.join(directory, "store"))),
    CentralObjectStore.CentralObjectStore,
  );
  const counts = { uploads: 0 };
  const store = CentralObjectStore.CentralObjectStore.of({
    ...inner,
    write: (key, body, precondition) =>
      Effect.suspend(() => {
        counts.uploads += 1;
        return inner.write(key, body, precondition);
      }),
  });
  const services = Context.add(
    yield* Effect.context<FileSystem.FileSystem | Path.Path>(),
    CentralObjectStore.CentralObjectStore,
    store,
  );
  const workDir = path.join(directory, "work");
  const claudeHome = path.join(directory, "claude");
  const codexHome = path.join(directory, "codex");
  /** relativePath is under <home>/projects unless a root is given. */
  const file = (home: string, relativePath: string, root = "projects") =>
    path.join(home, root, ...relativePath.split("/"));

  let mtime = 1_800_000_000;
  /** Whole seconds, so every write gets a distinct mtime. */
  const writeAt = (absolutePath: string, contents: string, flag: "w" | "a" = "w") => {
    NodeFS.mkdirSync(path.dirname(absolutePath), { recursive: true });
    NodeFS.writeFileSync(absolutePath, contents, { flag });
    mtime += 1;
    NodeFS.utimesSync(absolutePath, mtime, mtime);
  };
  const write = (relativePath: string, contents: string, flag: "w" | "a" = "w") =>
    writeAt(file(claudeHome, relativePath), contents, flag);

  /** Runs collect and returns the section with the number of blobs it uploaded. */
  const collectWith = <Section>(
    layout: ProviderTranscripts.Layout<Section>,
    previous: Section,
    home: string,
  ) =>
    Effect.gen(function* () {
      const before = counts.uploads;
      const roots = yield* ProviderTranscripts.collect({
        home,
        roots: layout.roots,
        previous: layout.toFiles(previous),
        upload: ({ path: source, offset, length }) =>
          Blobs.upload({
            homeKey,
            workDir,
            path: source,
            offset,
            length,
            isKnown: () => false,
          }).pipe(Effect.provideContext(services)),
      });
      return { section: layout.fromFiles(roots), uploads: counts.uploads - before };
    });
  const collect = (previous: typeof ClaudeSection.Type, home = claudeHome) =>
    collectWith(ProviderTranscripts.claude, previous, home);

  const stageWith = <Section>(
    layout: ProviderTranscripts.Layout<Section>,
    section: Section,
    home: string,
  ) =>
    ProviderTranscripts.stage({
      home,
      files: layout.toFiles(section),
      stagingDir: path.join(home, ".central-restore-test"),
      download: ({ ref, destinationPath, append }) =>
        Blobs.download({ homeKey, workDir, ref, destinationPath, append }).pipe(
          Effect.provideContext(services),
        ),
    });

  const stage = (section: typeof ClaudeSection.Type, home: string) =>
    stageWith(ProviderTranscripts.claude, section, home);

  return {
    directory,
    claudeHome,
    codexHome,
    file,
    writeAt,
    write,
    collect,
    collectWith,
    stage,
    stageWith,
  };
});

it.layer(NodeServices.layer)("ProviderTranscripts", (it) => {
  it.effect("uploads only complete appended lines and reuses earlier chunks", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;

      fixture.write(transcript, '{"n":1}\n{"n":2');
      const first = yield* fixture.collect(empty);
      assert.strictEqual(first.uploads, 1);
      const firstEntry = first.section.files[transcript]!;
      assert.strictEqual(firstEntry.size, 8);
      assert.strictEqual(firstEntry.chunks.length, 1);

      const unchanged = yield* fixture.collect(first.section);
      assert.strictEqual(unchanged.uploads, 0);
      assert.deepStrictEqual(unchanged.section, first.section);

      fixture.write(transcript, "}\n", "a");
      const appended = yield* fixture.collect(first.section);
      assert.strictEqual(appended.uploads, 1);
      const appendedEntry = appended.section.files[transcript]!;
      assert.strictEqual(appendedEntry.size, 16);
      assert.deepStrictEqual(appendedEntry.chunks[0], firstEntry.chunks[0]);
      assert.deepStrictEqual(
        appendedEntry.chunks.map((chunk) => chunk.size),
        [8, 8],
      );

      const again = yield* fixture.collect(appended.section);
      assert.strictEqual(again.uploads, 0);
      assert.strictEqual(again.section.files[transcript], appendedEntry);
    }),
  );

  it.effect("rewrites a transcript whose prefix changed or whose chunks reached the cap", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;

      fixture.write(transcript, "a\nb\n");
      const first = yield* fixture.collect(empty);
      fixture.write(transcript, "x\nb\nc\n");
      const rewritten = yield* fixture.collect(first.section);
      assert.strictEqual(rewritten.uploads, 1);
      assert.deepStrictEqual(
        rewritten.section.files[transcript]!.chunks.map((chunk) => chunk.size),
        [6],
      );

      let section = rewritten.section;
      while (section.files[transcript]!.chunks.length < TRANSCRIPT_MAX_CHUNKS) {
        fixture.write(transcript, "line\n", "a");
        section = (yield* fixture.collect(section)).section;
      }
      fixture.write(transcript, "last\n", "a");
      const compacted = yield* fixture.collect(section);
      assert.strictEqual(compacted.uploads, 1);
      const entry = compacted.section.files[transcript]!;
      assert.strictEqual(entry.chunks.length, 1);
      assert.strictEqual(entry.chunks[0]!.size, entry.size);
      assert.strictEqual(
        entry.size,
        NodeFS.statSync(fixture.file(fixture.claudeHome, transcript)).size,
      );
    }),
  );

  it.effect("syncs other files whole, skips symlinks and drops deleted files", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;

      fixture.write(transcript, "a\n");
      fixture.write(toolResult, "no trailing newline");
      NodeFS.symlinkSync(
        fixture.directory,
        fixture.file(fixture.claudeHome, "-workspace-app/outside"),
      );
      const first = yield* fixture.collect(empty);
      assert.deepStrictEqual(Object.keys(first.section.files).sort(), [transcript, toolResult]);
      assert.strictEqual(first.section.files[toolResult]!.size, 19);

      NodeFS.rmSync(fixture.file(fixture.claudeHome, transcript));
      const second = yield* fixture.collect(first.section);
      assert.strictEqual(second.uploads, 0);
      assert.deepStrictEqual(Object.keys(second.section.files), [toolResult]);

      // Codex's login, config, history, logs and state database never leave the machine.
      const path = yield* Path.Path;
      for (const name of [
        "auth.json",
        "auth.lock",
        "config.toml",
        "history.jsonl",
        "session_index.jsonl",
        "state_5.sqlite",
        "log/codex-tui.log",
        "shell_snapshots/snapshot.sh",
      ]) {
        fixture.writeAt(path.join(fixture.codexHome, name), "local\n");
      }
      const none = yield* fixture.collectWith(
        ProviderTranscripts.codex,
        undefined,
        fixture.codexHome,
      );
      assert.isUndefined(none.section);
      fixture.writeAt(fixture.file(fixture.codexHome, rollout, "sessions"), "a\n");
      fixture.writeAt(fixture.file(fixture.codexHome, archivedRollout, "archived_sessions"), "b\n");
      const codex = yield* fixture.collectWith(
        ProviderTranscripts.codex,
        undefined,
        fixture.codexHome,
      );
      assert.deepStrictEqual(Object.keys(codex.section?.sessions ?? {}), [rollout]);
      assert.deepStrictEqual(Object.keys(codex.section?.archivedSessions ?? {}), [archivedRollout]);
    }),
  );

  it.effect("stages and places transcripts that the next collect reuses without reading", () =>
    Effect.gen(function* () {
      const fixture = yield* setup;
      const path = yield* Path.Path;

      fixture.write(transcript, "a\nb\npartial");
      fixture.write(toolResult, "result");
      const { section } = yield* fixture.collect(empty);

      const restoredHome = path.join(fixture.directory, "restored");
      const { place } = yield* fixture.stage(section, restoredHome);
      assert.isFalse(NodeFS.existsSync(path.join(restoredHome, "projects")));
      yield* place;

      assert.strictEqual(
        NodeFS.readFileSync(fixture.file(restoredHome, transcript), "utf8"),
        "a\nb\n",
      );
      assert.strictEqual(
        NodeFS.readFileSync(fixture.file(restoredHome, toolResult), "utf8"),
        "result",
      );
      for (const [relativePath, entry] of Object.entries(section.files)) {
        assert.strictEqual(
          NodeFS.statSync(fixture.file(restoredHome, relativePath)).mtimeMs,
          entry.mtimeMs,
        );
      }
      assert.isFalse(NodeFS.existsSync(path.join(restoredHome, ".central-restore-test")));

      const next = yield* fixture.collect(section, restoredHome);
      assert.strictEqual(next.uploads, 0);
      assert.deepStrictEqual(next.section, section);

      const codexRollout = fixture.file(fixture.codexHome, rollout, "sessions");
      fixture.writeAt(codexRollout, '{"type":"session_meta"}\n{"type":"response_item"}\npartial');
      fixture.writeAt(fixture.file(fixture.codexHome, archivedRollout, "archived_sessions"), "x\n");
      const codex = (yield* fixture.collectWith(
        ProviderTranscripts.codex,
        undefined,
        fixture.codexHome,
      )).section;
      const restoredCodex = path.join(fixture.directory, "restored-codex");
      yield* (yield* fixture.stageWith(ProviderTranscripts.codex, codex, restoredCodex)).place;
      assert.strictEqual(
        NodeFS.readFileSync(fixture.file(restoredCodex, rollout, "sessions"), "utf8"),
        '{"type":"session_meta"}\n{"type":"response_item"}\n',
      );
      assert.strictEqual(
        NodeFS.readFileSync(
          fixture.file(restoredCodex, archivedRollout, "archived_sessions"),
          "utf8",
        ),
        "x\n",
      );
      const nextCodex = yield* fixture.collectWith(ProviderTranscripts.codex, codex, restoredCodex);
      assert.strictEqual(nextCodex.uploads, 0);
      assert.deepStrictEqual(nextCodex.section, codex);
    }),
  );
});
