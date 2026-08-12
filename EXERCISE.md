# Coding exercise: from notebook to flow

**Time budget: 4 hours.** Language: Python. You may use any libraries you like.

---

## 1. The problem

Scientists prototype in notebooks. They pull some data, plot it, tweak a threshold, re-run,
tweak it again, plot again. After a week the notebook works — and it is also the only place
that knowledge lives. Nobody can run it on a schedule, nobody can run it on a different
sample, and nobody can tell which of the 40 cells matter.

Turning that notebook into a **parameterised, production flow** is the step that almost never
happens, because it's tedious and nobody remembers which numbers were meaningful and which
were accidents of the last run.

But the notebook itself has already told you. Over a week of iteration:

- **Cells that ran many times, unchanged, are the pipeline.** They're the load-bearing steps.
- **Cells whose code kept changing structurally are still experiments.** They're not ready.
- **Values the scientist kept editing — a threshold, a window length, a device name — are
  the parameters.** Nobody edits a constant fifteen times unless it's really a knob.

Your task is to build the module that learns this from execution history and proposes the
graduation: which cells become the flow body, which literals become flow parameters, and what
the resulting script looks like.

This exercise is set in **Balthazar**, a lab-automation platform. Flows are Python scripts that
run on a Runner and are orchestrated from a web UI; they read and write **Devices** (samples,
wafers, cells) and produce **Runs**. You do not need to know Balthazar to do this exercise —
everything you need is in §3 and the starter code.

---

## 2. What we're evaluating

Roughly in order of weight:

1. **Judgement about evidence.** When is a signal strong enough to act on? What do you do with
   three data points? A confident recommendation from thin evidence is worse than an honest
   "not enough runs yet."
2. **Separating the deterministic core from the LLM.** The statistics should be reproducible
   and testable. The model should do the things a model is good at.
3. **Code quality** under time pressure: clear structure, honest error handling, tests where
   they earn their place.
4. **Communication.** Your `REPORT.md` (§8) matters as much as the code.

We are **not** evaluating how much you finish. A tightly-scoped Tier 1 + Tier 2 with good tests
and a clear write-up beats a sprawling half-working Tier 4. Say what you cut and why.

---

## 3. What you are given

Clone the repo. You get a working tunnel and pre-recorded telemetry, so you do not have to
build infrastructure before you can start on the interesting part.

```
flows/tunnel_server.py          Balthazar-side tunnel (v1) — runs as a flow
flows/tunnel_session_server.py  Balthazar-side tunnel (v2) — open run contexts
balthazar.py                    Local client shim for v1
session_tunnel/balthazar.py     Local client shim for v2
demo.ipynb / demo.py            Worked examples against a live space
fixtures/
├── notebook/
│   ├── rev1.ipynb              The same notebook at three points in its life
│   ├── rev2.ipynb
│   └── rev3.ipynb
├── sessions/*.jsonl            ~5 recorded sessions of execution telemetry
└── expected/                   One hand-written reference recommendation
tests/                          A few smoke tests to build on
```

**The fixtures are the point.** They let you develop and test the whole analysis and codegen
path offline, deterministically, with no Balthazar instance and no API key. Use them as your
primary development loop. A live tunnel is available if you want it (§4) but is not required
for any tier.

### The tunnel, in one paragraph

A flow running on the Runner hosts a loopback JSON-RPC server. A local module named `balthazar`
mimics the Runner-injected API and forwards calls to it. So a local script — in your editor,
with breakpoints — can read real Devices and write real Runs. Naming the local module
`balthazar` is safe: the Runner registers its own module as a CPython *builtin*, and
`BuiltinImporter` precedes `PathFinder`, so the real module always wins on the Runner. The v2
tunnel additionally keeps flow-run *contexts* open across calls, so `with
blt.enter_new_flow_run(...)` works locally, with plots and outputs landing on the child run.

---

## 4. Part A — the tunnel (foundation, tiered)

**Budget ~45 minutes. Do not exceed one hour.** Part A exists to prove you can operate and
extend the plumbing, and to give your AI module a real place to write its findings. It is not
where the marks are.

| Tier | Task |
|---|---|
| **A1 — required** | Get the v1 tunnel running against a live space (or against the provided fake Runner — `tests/fake_runtime/`, which reproduces the Runner's quirks). Add **one** RPC operation: `record_execution`, which accepts a telemetry event (§6) and appends it to a durable store. Show it working end-to-end. |
| **A2** | Have your module write its recommendation back to Balthazar: create a flow run whose `parameters` are the inferred flow parameters and whose `output` carries the summary metrics. Attach at least one plot. |
| **A3** | Use the v2 session tunnel: wrap the analysis in `enter_new_flow_run(...)` so each analysed notebook gets its own child run, nested under a parent run for the whole batch. |
| **A4** | Write the recommendation as a Balthazar wiki page, or a device-parameter update on the devices the notebook touched. |

Two constraints that will bite if you ignore them, both documented in the starter code:

- `enter_new_flow_run` and its exit are **main-thread only** on the Runner. The tunnel serves
  HTTP on worker threads, so all `blt.*` work goes through a job queue drained by the main
  thread. If you add an operation that opens a context, it must go through that queue.
- `device.params` is a server-synced proxy, not a dict. Write with `params.update({...})`;
  subscript assignment does not reliably sync, and `pop(key, default)` raises `KeyError`
  instead of honouring the default.

---

## 5. Part B — the AI module (the main task)

Build a module — we'll call it `graduate/` — that learns from execution telemetry and proposes
how to turn a notebook into a parameterised flow. It can live anywhere and run however you
like: a library plus a CLI is the obvious shape.

```bash
graduate record   --notebook fixtures/notebook/rev3.ipynb   # capture executions
graduate analyse  --sessions fixtures/sessions/             # produce a recommendation
graduate emit     --out flows/generated_flow.py             # write the flow
graduate accept   --param bias_max_v                        # feedback (Tier 4)
```

### Tier 1 — Capture

Record executions. For notebooks, hook IPython's `pre_run_cell` / `post_run_cell` events
(`%load_ext graduate`) and emit one event per execution: what ran, when, how long, whether it
raised. Persist as JSONL (§6).

Fixtures already contain recorded sessions, so **Tier 1 is not a blocker for Tiers 2–4.** If
you're short on time, read the fixtures and come back to the recorder.

### Tier 2 — Analyse (the core of the exercise)

From a set of recorded sessions, decide for each cell what it *is*, and which of its literals
are parameters. This is the part we care most about; §7 discusses the hard bits.

Output a structured recommendation (§6) that classifies every cell and lists every proposed
parameter with its inferred type and evidence.

### Tier 3 — Emit

Generate a runnable Balthazar flow from the recommendation:

- one `def <name>_flow():` containing the promoted cells, **ordered by data dependency** (not
  by cell position — the scientist ran cells out of order and you must not reproduce that)
- inferred parameters read via `blt.params.get("name", default)` at the top
- imports hoisted to module level
- summary values written to `blt.output` (primitives only: str / int / float / bool / small list)
- `if __name__ == "__main__":` guard, so an orchestrator can import the function without
  executing it
- cells you classified as experimental left out, and listed in a comment saying why

The output must be syntactically valid (`ast.parse`) and importable without a Runner present.

### Tier 4 — Learn

"Learn" means the recommendation improves as evidence accumulates, and as the user reacts to it.
Pick at least one:

- **Evidence growth.** Re-running `analyse` over 2, then 3, then 5 sessions should visibly
  change confidence and may change classifications. Show this — a table or a plot.
- **Feedback.** `graduate accept` / `reject` records the user's decision and later analyses
  respect it: a rejected parameter is not re-proposed, an accepted one is promoted at a lower
  evidence bar. Persist this and show it changing behaviour on a second run.
- **Cross-notebook priors.** If the same literal is a parameter in three notebooks, propose it
  faster in the fourth.

---

## 6. Data contracts

Two schemas are fixed so your work is comparable against the fixtures. Everything else is yours.

**Telemetry event** (one JSON object per line):

```json
{
  "schema": 1,
  "session_id": "2026-08-11T09:14:03Z-a91f",
  "seq": 17,
  "notebook": "fixtures/notebook/rev3.ipynb",
  "cell_id": "83524bc2",
  "source": "bias = np.linspace(-1.0, 1.0, 201)\ncurrent = sweep(bias, threshold=0.42)\n",
  "started_at": "2026-08-11T09:31:44.120Z",
  "duration_ms": 812,
  "status": "ok",
  "error_type": null,
  "flow_run_id": "019ce6f1-c7de-7e72-91d2-0a90a1269533"
}
```

`status` is `ok` | `error` | `interrupted`. `cell_id` is the notebook cell id (stable in
nbformat ≥ 4.5) and may be absent for script executions. `flow_run_id` is present when the
execution happened inside a tunnel flow run.

**Recommendation** — you choose the exact shape, but it must contain, per cell: the
classification, the evidence that supports it, and a confidence; and per proposed parameter:
name, inferred type, default, observed values, and which cell it came from. `fixtures/expected/`
has one hand-written example to calibrate against. Emitting JSON *and* a human-readable summary
is a good idea; a recommendation nobody can read doesn't get adopted.

---

## 7. The hard parts

These are the questions the exercise is really about. We're not looking for one right answer —
we want to see how you reason about them, and we want your `REPORT.md` to say what you chose.

**A cell's text changes between executions. Is it still the same cell?** `cell_id` survives
edits but not copy-paste, splitting, or merging — and a notebook that was rewritten between
sessions may have entirely new ids for morally identical cells. What makes two executions
"the same step"? What can you compute from the source itself that is stable under the edits
you'd expect a scientist to make, and unstable under the edits you wouldn't?

**Two cells changed a lot. Are they the same kind of "a lot"?** One had its threshold edited
from `0.3` to `0.35` to `0.42` across nine runs. The other was rewritten three times as the
scientist tried different smoothing approaches. Both look like churn if you diff the text.
Only one of them is telling you about a parameter. How do you tell them apart, mechanically?

**How much evidence is enough?** A literal that changed once might be a parameter or might be
a typo fix. A cell that ran 40 times in one session might be a pipeline step or might be
someone hammering shift-enter while debugging. What are your thresholds, and — more importantly
— how does the recommendation *communicate* its own uncertainty rather than hiding it?

**Order.** Cells were executed out of order, some more than once, some after the cell below
them. The generated flow needs a correct linear order. What defines it?

**What does the LLM add that statistics can't?** Be specific in your report. Naming things,
writing docstrings, explaining a recommendation in prose, and judging whether a block of code
is "a measurement step" or "scratch work" are all plausible answers. "Doing the analysis" is
not — see §9.

---

## 8. LLM integration requirements

The AI module must use an LLM, and must satisfy three constraints:

**1. The deterministic layer is the source of truth.** Frequency, churn, parameter candidates,
and dependency order are computed in code. The model may name, explain, describe, judge
borderline cases, and write the final prose — it must not be the thing that decides which cells
ran most often. If the model's suggestion conflicts with the computed evidence, the evidence
wins and your code should be able to say so.

**2. It degrades without an API key.** With no credentials, `analyse` and `emit` must still
produce a complete, valid recommendation and a runnable flow — with mechanical names
(`param_threshold_0`) and no prose. We will run your code both ways. A module that crashes or
silently produces nothing without a key fails this requirement.

**3. Structured where structure matters.** Anything you parse must come back in a shape you can
rely on, not scraped out of prose.

Practical notes, current as of writing:

```python
import anthropic  # pip install anthropic

client = anthropic.Anthropic()          # reads ANTHROPIC_API_KEY
response = client.messages.create(
    model="claude-opus-5",
    max_tokens=4096,
    thinking={"type": "adaptive"},       # sensible default for judgement calls
    messages=[{"role": "user", "content": prompt}],
)
if response.stop_reason == "refusal":    # check before reading content
    ...
```

- `claude-opus-5` is the current default model. Use the official `anthropic` SDK, not raw HTTP.
- For reliable JSON, use `client.messages.parse(..., output_format=YourPydanticModel)`, or
  `output_config={"format": {"type": "json_schema", "schema": ...}}`.
- **Do not pass `temperature`, `top_p`, or `top_k`** — they are rejected with a 400 on this
  model. Steer with the prompt.
- Cache the stable part of your prompt if you call the model per-cell; a prefix ≥ 512 tokens is
  cacheable and the analysis prompt is very repetitive.
- Send the model **schema and structure, not raw lab data.** Cell source is fine; a device's
  measured values are not yours to ship. Say in your report where you drew that line.

An API key will be provided. If you'd rather not use one, stub the client behind an interface
and say so — we'll evaluate the seam, not the spend.

---

## 9. Out of scope

Deliberately excluded, so you don't spend time there:

- **Any UI.** CLI output and generated files only.
- **Training or fine-tuning a model.** "Learn" here means accumulating and using evidence
  (§5, Tier 4), not gradient descent. If you reach for scikit-learn, be able to justify it over
  counting.
- **Editing notebooks in place, or a full notebook↔script round trip.** One direction only.
- **Full Python semantics.** Your dependency analysis will not handle `exec`, `globals()`,
  star-imports, or decorators that rewrite signatures. Detect what you can, and *say* what you
  don't handle rather than failing silently.
- **Making the tunnel production-ready.** Auth, multi-tenancy, and recovery are already
  handled or explicitly out of scope.

---

## 10. Deliverables

1. **Working code**, runnable from a clean checkout with documented setup.
2. **`REPORT.md`**, ~1–2 pages:
   - which tiers you completed, and what you cut
   - your answers to §7 — the identity, churn, and evidence questions — and why
   - where the LLM sits, and what happens without it
   - what breaks first as this scales: more cells, longer histories, a notebook that
     genuinely has no stable core
   - what you'd do with another day
3. **A demonstration.** Either `graduate analyse fixtures/sessions/ && graduate emit`, with the
   generated flow committed, or a short screen recording. Show it working on the fixtures; if
   you also ran it against a live space, show that too.
4. **Tests** for whatever you'd be nervous to refactor. The analysis layer is deterministic and
   the fixtures are fixed, so it is straightforwardly testable — we will look here.

---

## 11. Suggested pacing

Advisory, not prescriptive.

| Time | |
|---|---|
| 0:00–0:20 | Read the fixtures. Run a session, look at the telemetry, look at the three notebook revisions side by side. Form a hypothesis before writing code. |
| 0:20–0:50 | Part A1: tunnel running, `record_execution` added. |
| 0:50–2:00 | Tier 2: the analyser. This is the exercise — give it the most time. |
| 2:00–2:45 | Tier 3: codegen. |
| 2:45–3:15 | LLM layer, with the no-key path working. |
| 3:15–3:35 | Tier 4 if you have it, or tests and cleanup if you don't. |
| 3:35–4:00 | `REPORT.md`. Do not skip this to add one more feature. |

If you're behind at 2:00, cut Tier 4 and the LLM prose, and ship a solid analyser plus a
generated flow. That's a complete answer.

---

## 12. Stretch, if you finish early

- **Confidence over time.** A plot of how each classification's confidence evolved across the
  five sessions, written back to Balthazar as a visualisation.
- **Reverse direction.** Given a generated flow and a new session of telemetry, detect drift —
  the scientist has started editing a "stable" cell again, so it should be de-promoted.
- **Multi-notebook.** Three notebooks in the fixtures share a data-loading preamble. Detect it
  and propose a shared helper rather than duplicating it into three flows.
- **Cost.** Report tokens and spend per analysed notebook, and show one concrete optimisation
  you made.
