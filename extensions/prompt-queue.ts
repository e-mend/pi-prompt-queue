/**
 * prompt-queue — serialize prompts across pi sessions for one local model.
 *
 * Usage (any pi session, any project folder):
 *   /queue [model] <prompt>     enqueue a prompt (first token is a model override
 *                               if it resolves in the registry); runs at the head of the global FIFO queue
 *   /queue                      show the queue
 *   /queue-cancel <id|mine|all> cancel items
 *
 * All pi sessions on this machine share one queue file (default
 * ~/.pi/agent/prompt-queue, override with PI_QUEUE_DIR). Exactly one item
 * runs at a time, so a single llama-swap/local model serves every session
 * in turn — unattended. Each prompt runs in the session that queued it
 * (that session's context, project dir, and model).
 *
 * Self-healing: a "running" item whose owner process died is canceled
 * immediately (a stalled-but-alive owner is canceled after 90s without
 * heartbeat); a "queued" item whose owner session/process/pump is gone is
 * canceled. Re-queue the prompt in a live session to retry.
 *
 * ponytail: FIFO is per-enqueue-time; two sessions enqueuing in the same
 * millisecond may swap order. Fine for one human.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import type { ExtensionAPI, ExtensionCommandContext } from "@earendil-works/pi-coding-agent";

const QDIR = process.env.PI_QUEUE_DIR ?? path.join(os.homedir(), ".pi", "agent", "prompt-queue");
const ITEMS_FILE = path.join(QDIR, "items.json");
const LOCK_FILE = path.join(QDIR, "lock");

const STALE_RUN_MS = 90_000; // running item with no heartbeat for 90s -> owner died
const STALE_PUMP_MS = 60_000; // owner pid alive but pump file stale -> pump dead (e.g. extension reload)
const HEARTBEAT_MS = 15_000;
const POLL_MS = 2_000;
const START_WAIT_MS = 20_000; // grace for a sent prompt to actually start a run
const HISTORY = 20; // finished items kept for display
const MAX_RETRIES = 3; // model-error retries per item (llama-swap reload race)
const RETRY_COOLDOWN_MS = 15_000; // wait before retrying so the model settles

type Status = "queued" | "running" | "done" | "canceled";

interface Item {
  id: string;
  owner: string; // `${pid}:${token}` of the enqueuing pi process
  pid: number;
  session: string; // session file path (chat identity)
  label: string; // cwd (project folder)
  prompt: string;
  model?: string; // "provider/model-id" override
  status: Status;
  heartbeat?: number; // updated while running
  enqueuedAt: number;
  startedAt?: number;
  doneAt?: number;
  note?: string; // cancel reason
  retries?: number; // model-error retries used
}

const TOKEN = Math.random().toString(36).slice(2, 10);
const ME = `${process.pid}:${TOKEN}`;
// Keyed by ME (pid:token), not pid: a /reload re-evaluates the extension in the
// same process (same pid, new token). A pid-keyed file would stay fresh after
// reload, so old items would never be claimed (new ME) nor canceled (fresh
// pump file) — a single orphaned head item would deadlock the whole queue.
const PUMP_FILE = path.join(QDIR, `pump-${ME}`);

let pumping = false;
let liveCtx: ExtensionCommandContext | undefined; // refreshed by each /queue so a replaced session is picked up

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function readItems(): Item[] {
  try {
    const raw: unknown = JSON.parse(fs.readFileSync(ITEMS_FILE, "utf8"));
    return Array.isArray(raw) ? (raw as Item[]) : [];
  } catch {
    return [];
  }
}

function writeItems(items: Item[]) {
  fs.mkdirSync(QDIR, { recursive: true });
  const tmp = `${ITEMS_FILE}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, JSON.stringify(items, null, 2));
  fs.renameSync(tmp, ITEMS_FILE);
}

/** Short-lived exclusive lock for read-modify-write of items.json. */
async function withLock<T>(fn: () => T): Promise<T> {
  for (;;) {
    try {
      fs.mkdirSync(QDIR, { recursive: true });
      fs.writeFileSync(LOCK_FILE, ME, { flag: "wx" });
      break;
    } catch (e) {
      if ((e as NodeJS.ErrnoException).code !== "EEXIST") throw e;
      try {
        if (Date.now() - fs.statSync(LOCK_FILE).mtimeMs > 10_000) fs.unlinkSync(LOCK_FILE); // steal stale
      } catch {
        /* lock vanished */
      }
      await sleep(50);
    }
  }
  try {
    return fn();
  } finally {
    try {
      fs.unlinkSync(LOCK_FILE);
    } catch {
      /* already gone */
    }
  }
}

function pidAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch (e) {
    return (e as NodeJS.ErrnoException).code === "EPERM";
  }
}

function pumpAlive(owner: string): boolean {
  try {
    return Date.now() - fs.statSync(path.join(QDIR, `pump-${owner}`)).mtimeMs < STALE_PUMP_MS;
  } catch {
    return false;
  }
}

function touchPump() {
  try {
    fs.mkdirSync(QDIR, { recursive: true });
    fs.writeFileSync(PUMP_FILE, String(Date.now()));
  } catch {
    /* non-fatal */
  }
}

function isActive(it: Item): boolean {
  return it.status === "queued" || it.status === "running";
}

function truncate(s: string, n = 60): string {
  s = s.replace(/\s+/g, " ").trim();
  return s.length > n ? `${s.slice(0, n - 1)}…` : s;
}

function fmtAge(from: number): string {
  const s = Math.max(0, Math.round((Date.now() - from) / 1000));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m`;
  return `${Math.floor(m / 60)}h${m % 60}m`;
}

/** Cancel dead/stuck items. Caller holds the lock. */
function cleanup(items: Item[]): { items: Item[]; canceled: Item[] } {
  const canceled: Item[] = [];
  for (const it of items) {
    if (it.status === "running") {
      if (!pidAlive(it.pid)) {
        it.status = "canceled";
        it.note = "owner died mid-run";
        canceled.push(it);
      } else if (Date.now() - (it.heartbeat ?? it.startedAt ?? 0) > STALE_RUN_MS) {
        it.status = "canceled";
        it.note = "stale — pump stalled mid-run";
        canceled.push(it);
      }
    } else if (it.status === "queued" && (!pidAlive(it.pid) || !pumpAlive(it.owner))) {
      it.status = "canceled";
      it.note = pidAlive(it.pid) ? "owner stopped pumping (reload?)" : "owner session gone";
      canceled.push(it);
    }
  }
  const finished = items.filter((i) => i.status === "done" || i.status === "canceled");
  if (finished.length > HISTORY) {
    const keep = new Set(finished.slice(-HISTORY).map((i) => i.id));
    return { items: items.filter((i) => keep.has(i.id) || isActive(i)), canceled };
  }
  return { items, canceled };
}

/** Claim the head item if it is mine. Caller holds the lock. */
function tryClaim(items: Item[], me: string): Item | undefined {
  const head = items.find(isActive);
  if (head && head.status === "queued" && head.owner === me) {
    head.status = "running";
    head.startedAt = Date.now();
    head.heartbeat = Date.now();
    return head;
  }
  return undefined;
}

async function cancelItem(id: string, note: string) {
  await withLock(() => {
    const items = readItems();
    const it = items.find((i) => i.id === id);
    if (it && isActive(it)) {
      it.status = "canceled";
      it.note = note;
      it.doneAt = Date.now();
      writeItems(items);
    }
  });
}

/** User-message texts on the current branch (best effort). */
function userMessageTexts(ctx: ExtensionCommandContext): string[] {
  const out: string[] = [];
  try {
    const branch = ctx.sessionManager.getBranch() as Array<{
      type: string;
      message?: { role?: string; content?: unknown };
    }>;
    for (const e of branch) {
      if (e.type !== "message" || e.message?.role !== "user") continue;
      const c = e.message.content;
      if (typeof c === "string") {
        out.push(c);
      } else if (Array.isArray(c)) {
        out.push(
          (c as Array<{ type: string; text?: string }>)
            .filter((p) => p.type === "text" && typeof p.text === "string")
            .map((p) => p.text as string)
            .join("\n"),
        );
      }
    }
  } catch {
    /* best effort */
  }
  return out;
}

function countLines(p: string): number {
  try {
    return fs.readFileSync(p, "utf8").split("\n").filter(Boolean).length;
  } catch {
    return 0;
  }
}

/** True if a user message matching `prompt` was appended to the session file after line `fromLine`. */
function promptLandedInFile(session: string, fromLine: number, prompt: string): boolean {
  try {
    const lines = fs.readFileSync(session, "utf8").split("\n").filter(Boolean);
    const prefix = prompt.slice(0, 40);
    for (let i = fromLine; i < lines.length; i++) {
      try {
        const e: { message?: { role?: string; content?: unknown } } = JSON.parse(lines[i]);
        const c = e.message?.role === "user" ? e.message.content : undefined;
        const t =
          typeof c === "string"
            ? c
            : Array.isArray(c)
              ? (c as Array<{ type: string; text?: string }>)
                  .filter((p) => p.type === "text" && typeof p.text === "string")
                  .map((p) => p.text as string)
                  .join("\n")
              : "";
        if (t && (t === prompt || t.startsWith(prefix) || prompt.startsWith(t.slice(0, 40)))) return true;
      } catch {
        /* skip unparseable line */
      }
    }
  } catch {
    /* file gone */
  }
  return false;
}

/** Error message of the last assistant entry appended to the session file since line `fromLine`, if any. */
function lastAssistantError(session: string, fromLine: number): string | undefined {
  try {
    const lines = fs.readFileSync(session, "utf8").split("\n").filter(Boolean);
    let lastErr: string | undefined;
    for (let i = fromLine; i < lines.length; i++) {
      try {
        const e: { message?: { role?: string; stopReason?: string; errorMessage?: string } } = JSON.parse(lines[i]);
        const m = e.message;
        if (m?.role === "assistant" && (m.stopReason === "error" || m.errorMessage)) {
          lastErr = m.errorMessage || "model error";
        }
      } catch {
        /* skip unparseable line */
      }
    }
    return lastErr;
  } catch {
    return undefined;
  }
}

async function waitUntilActive(ctx: ExtensionCommandContext, timeoutMs: number): Promise<boolean> {
  const t0 = Date.now();
  while (Date.now() - t0 < timeoutMs) {
    if (!ctx.isIdle()) return true;
    await sleep(200);
  }
  return false;
}

function resolveModel(ctx: ExtensionCommandContext, spec: string) {
  if (spec.includes("/")) {
    const [p, ...r] = spec.split("/");
    return ctx.modelRegistry.find(p, r.join("/"));
  }
  const cur = ctx.model?.provider;
  return cur ? ctx.modelRegistry.find(cur, spec) : undefined;
}

/**
 * Run one claimed item in this session. Returns false to stop the pump
 * (item was canceled out from under us), true to continue with the rest.
 */
async function runItem(pi: ExtensionAPI, ctx: ExtensionCommandContext, item: Item): Promise<boolean> {
  const hb = setInterval(() => {
    withLock(() => {
      const items = readItems();
      const it = items.find((i) => i.id === item.id);
      if (it && it.status === "running" && it.owner === ME) it.heartbeat = Date.now();
      writeItems(items);
    }).catch(() => {});
  }, HEARTBEAT_MS);
  try {
    ctx.ui.notify(`queue ▶ ${item.id} ${truncate(item.prompt)}`, "info");
    if (ctx.mode === "tui") ctx.ui.setStatus("queue", `▶ ${item.id} ${truncate(item.prompt, 30)}`);
    if (item.model) {
      const [p, ...r] = item.model.split("/");
      const model = ctx.modelRegistry.find(p, r.join("/"));
      if (!model || !(await pi.setModel(model))) {
        await cancelItem(item.id, `model ${item.model} unavailable`);
        ctx.ui.notify(`queue ✗ ${item.id}: model ${item.model} unavailable`, "error");
        return true;
      }
    }
    // Let any in-flight interactive work finish before taking the slot.
    await ctx.waitForIdle();
    const before = userMessageTexts(ctx);
    const linesBefore = countLines(item.session);
    pi.sendUserMessage(item.prompt); // fire-and-forget; verified below
    const started = await waitUntilActive(ctx, START_WAIT_MS);
    if (started) await ctx.waitForIdle();
    // Verify the prompt actually landed (sendUserMessage swallows errors).
    const after = userMessageTexts(ctx);
    const prefix = item.prompt.slice(0, 40);
    // In-memory branch check plus a file check: the branch leaf can move out
    // from under us (tree navigation, session replacement, shutdown mid-run),
    // but the session file is append-only and is the source of truth.
    const sent =
      (after.length > before.length &&
        after.some((t) => t === item.prompt || t.startsWith(prefix) || item.prompt.startsWith(t.slice(0, 40)))) ||
      promptLandedInFile(item.session, linesBefore, item.prompt);
    if (!sent) {
      const note = started ? "run finished but prompt not found in session" : "prompt did not start a run (send failed)";
      await cancelItem(item.id, note);
      ctx.ui.notify(`queue ✗ ${item.id}: ${note}`, "error");
      return true;
    }
    // Model error (e.g. llama-swap reloading mid-stream): re-queue with cooldown.
    const modelErr = lastAssistantError(item.session, linesBefore);
    if (modelErr) {
      const retries = (item.retries ?? 0) + 1;
      if (retries > MAX_RETRIES) {
        await cancelItem(item.id, `model error after ${MAX_RETRIES} retries: ${truncate(modelErr, 60)}`);
        ctx.ui.notify(`queue ✗ ${item.id}: ${modelErr}`, "error");
        return true;
      }
      const requeued = await withLock(() => {
        const items = readItems();
        const it = items.find((i) => i.id === item.id);
        if (!it || it.status !== "running" || it.owner !== ME) return false;
        it.status = "queued";
        delete it.startedAt;
        delete it.heartbeat;
        it.retries = retries;
        writeItems(items);
        return true;
      });
      if (requeued) {
        ctx.ui.notify(`queue ↻ ${item.id}: model error (${truncate(modelErr, 40)}) — retry ${retries}/${MAX_RETRIES}`, "warning");
        await sleep(RETRY_COOLDOWN_MS);
        return true;
      }
    }
    // Mark done only if we still own it (it may have been canceled mid-run).
    const stillMine = await withLock(() => {
      const items = readItems();
      const it = items.find((i) => i.id === item.id);
      if (!it || it.status !== "running" || it.owner !== ME) return false;
      it.status = "done";
      it.doneAt = Date.now();
      writeItems(items);
      return true;
    });
    if (!stillMine) {
      ctx.ui.notify(`queue: ${item.id} was canceled while running — stopping this session's pump`, "warning");
      return false;
    }
    ctx.ui.notify(`queue ✓ ${item.id} done`, "info");
    return true;
  } catch (e) {
    const msg = e instanceof Error ? e.message : String(e);
    await cancelItem(item.id, `error: ${msg}`);
    ctx.ui.notify(`queue ✗ ${item.id}: ${msg}`, "error");
    return true;
  } finally {
    clearInterval(hb);
  }
}

/**
 * Per-process pump: claim-and-run this session's items in FIFO order until
 * it has none left. Fire-and-forget, so the session stays interactive while
 * waiting for its turn.
 */
async function pump(pi: ExtensionAPI) {
  touchPump();
  const hb = setInterval(touchPump, HEARTBEAT_MS);
  try {
    for (;;) {
      const ctx = liveCtx;
      if (!ctx) break;
      const { claimed, mineLeft, canceled } = await withLock(() => {
        const { items: cleaned, canceled } = cleanup(readItems());
        const claimed = tryClaim(cleaned, ME);
        const mineLeft = cleaned.some((i) => i.status === "queued" && i.owner === ME);
        writeItems(cleaned);
        touchPump();
        return { claimed, mineLeft, canceled };
      });
      for (const c of canceled) if (c.owner === ME) ctx.ui.notify(`queue ✗ ${c.id} canceled: ${c.note}`, "warning");
      if (claimed) {
        if (!(await runItem(pi, ctx, claimed))) break;
      } else if (!mineLeft) {
        break;
      } else {
        if (ctx.mode === "tui") {
          // Same text as the enqueue feedback, so refreshing it never loses
          // the "queued" confirmation (pi's status bar is latest-wins).
          const items = readItems();
          const mine = items.find((i) => i.status === "queued" && i.owner === ME);
          const pos = items.filter(isActive).findIndex((i) => i.owner === ME) + 1;
          if (mine) ctx.ui.setStatus("queue", `⏳ ${mine.id} queued (position ${pos})`);
        }
        await sleep(POLL_MS);
      }
    }
  } finally {
    clearInterval(hb);
    const ctx = liveCtx;
    pumping = false;
    liveCtx = undefined;
    try {
      fs.unlinkSync(PUMP_FILE);
    } catch {
      /* already gone */
    }
    if (ctx && ctx.mode === "tui" && readItems().filter(isActive).length === 0) {
      ctx.ui.setStatus("queue", undefined);
    }
  }
}

function showQueue(ctx: ExtensionCommandContext) {
  const items = readItems();
  const act = items.filter(isActive);
  const home = os.homedir();
  const short = (p: string) => (p.startsWith(home) ? `~${p.slice(home.length)}` : p);
  const lines: string[] = [];
  if (act.length === 0) {
    lines.push("queue: empty");
  } else {
    const running = act.filter((i) => i.status === "running").length;
    const mine = act.filter((i) => i.owner === ME).length;
    lines.push(`queue: ${running} running, ${act.length - running} waiting${mine ? ` · mine: ${mine}` : ""}`);
    for (const it of act) {
      const mark = it.status === "running" ? "▶" : "·";
      const model = it.model ? ` [${it.model}]` : "";
      const t = it.status === "running" ? (it.startedAt ?? it.enqueuedAt) : it.enqueuedAt;
      lines.push(`  ${mark} ${it.id}  ${short(it.label)}  ${truncate(it.prompt, 44)}${model}  ${fmtAge(t)}`);
    }
  }
  for (const it of items.filter((i) => i.status === "done" || i.status === "canceled").slice(-5)) {
    lines.push(`  ${it.status === "done" ? "✓" : "✗"} ${it.id}  ${short(it.label)}  ${truncate(it.prompt, 36)}${it.note ? `  (${it.note})` : ""}`);
  }
  ctx.ui.notify(lines.join("\n"), "info");
}

export default function (pi: ExtensionAPI) {
  pi.registerCommand("queue", {
    description: "Shared single-model prompt queue. /queue [model] <prompt> enqueues; /queue shows the queue",
    handler: async (args, ctx) => {
      const arg = args.trim();
      if (!arg) {
        showQueue(ctx);
        return;
      }
      let prompt = arg;
      let model: string | undefined;
      // First token is a model override only if it resolves in the registry.
      // (The old "model: prompt" colon syntax could not express llama-swap
      // model ids like "llama-swap/qwen3-coder:30b", which contain colons.)
      const sp = arg.indexOf(" ");
      if (sp > 0) {
        const found = resolveModel(ctx, arg.slice(0, sp));
        if (found) {
          model = `${found.provider}/${found.id}`;
          prompt = arg.slice(sp + 1).trim();
        }
      }
      const item: Item = {
        id: Math.random().toString(36).slice(2, 6),
        owner: ME,
        pid: process.pid,
        session: ctx.sessionManager.getSessionFile() ?? "unknown",
        label: ctx.cwd,
        prompt,
        model,
        status: "queued",
        enqueuedAt: Date.now(),
      };
      const pos = await withLock(() => {
        const items = readItems();
        items.push(item);
        writeItems(items);
        touchPump();
        return items.filter(isActive).length;
      });
      // Status bar, not a transient notify: the pump's same-tick "queue ▶"
      // notify reuses pi's single in-place status slot and would overwrite
      // this before the first render for a head-of-queue item.
      if (ctx.mode === "tui") {
        ctx.ui.setStatus("queue", `⏳ ${item.id} queued (position ${pos})`);
      } else {
        ctx.ui.notify(`queued ${item.id} (position ${pos}): ${truncate(prompt)}`, "info");
      }
      liveCtx = ctx;
      if (!pumping) {
        pumping = true;
        void pump(pi).catch((e) => {
          pumping = false;
          liveCtx = undefined;
          ctx.ui.notify(`queue pump error: ${e instanceof Error ? e.message : String(e)}`, "error");
        });
      }
    },
  });

  pi.registerCommand("queue-cancel", {
    description: "Cancel queue items: /queue-cancel <id|mine|all>",
    handler: async (args, ctx) => {
      const sel = args.trim().toLowerCase();
      if (!sel) {
        ctx.ui.notify("Usage: /queue-cancel <id|mine|all>", "warning");
        return;
      }
      const n = await withLock(() => {
        const items = readItems();
        let count = 0;
        for (const it of items) {
          if (!isActive(it)) continue;
          if (sel === "all" || (sel === "mine" && it.owner === ME) || it.id === sel) {
            it.status = "canceled";
            it.note = "canceled by user";
            it.doneAt = Date.now();
            count++;
          }
        }
        if (count) writeItems(items);
        return count;
      });
      ctx.ui.notify(n ? `canceled ${n} item(s)` : "no matching items", n ? "info" : "warning");
    },
  });
}
