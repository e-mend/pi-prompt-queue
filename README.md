# pi-prompt-queue

Run many prompts unattended against a single local model (e.g. llama-swap).
Type `/queue <prompt>` in any pi session — from any project folder, any
terminal — and prompts run **one at a time, globally**, in FIFO order. Each
prompt runs in the session that queued it (that session's context, project
dir, and model).

## Install

```bash
pi install npm:pi-prompt-queue
```

or from git:

```bash
pi install git:github.com/e-mend/pi-prompt-queue
```

Restart pi (or start a new session) to load it.

## Usage

```text
/queue [model:] <prompt>    enqueue a prompt; runs when it reaches the head
/queue                      show the queue
/queue-cancel <id|mine|all> cancel items
```

- `model:` is optional — `unc-q6k: fix the parser` (or `llama-swap/unc-q6k: …`)
  switches that one prompt to another model before running it.
- The session stays interactive while queued items wait; a status line shows
  your queue position, and you get a notification when each item starts and
  finishes.

## How it works

- All pi sessions on the machine share one queue file:
  `~/.pi/agent/prompt-queue/` (override with `PI_QUEUE_DIR`).
- Exactly one item runs at a time; the rest wait in FIFO order by enqueue time.
- Self-healing: if a session is closed, its pending items are canceled and the
  queue keeps going. A running item whose owner died is canceled immediately;
  a stalled owner is canceled after 90 s without heartbeat.
- If the model errors mid-run (e.g. llama-swap reloading the model), the item
  is retried up to 3 times with a 15 s cooldown.

## Notes

- Closing a terminal cancels its pending items — re-queue to retry.
- No runtime dependencies; state is plain JSON on disk.
