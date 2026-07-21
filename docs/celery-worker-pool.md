# Celery worker pool — prefork over eventlet

- **Status:** Accepted
- **Date:** 2026-07-21
- **Component:** request-processor worker (`request-processor/docker-entrypoint.sh`)

## Context

The request-processor worker processes async submissions (file/URL checks and
add-data) on Celery, consuming from SQS and running on Fargate ECS tasks
(currently 2 vCPU / 4 GB each, `desired_count = 2`).

We want to be able to **scale this workload up** — process more submissions
concurrently, and give each ECS task more compute — as demand grows. That means
the pool choice needs to (a) turn CPU into throughput predictably, and (b) let
us safely bound runaway tasks so scaling up doesn't just scale up the blast
radius of a bad job.

The worker had been running the **eventlet** pool (`-P eventlet`). A few things
made that a poor fit going forward:

1. **The workload is becoming more CPU/DB-bound than I/O-bound now that add_data has been introduced.** eventlet's strength
   is cheap high-concurrency for network-bound work (many green threads in one
   process). But these tasks spend most of their time on transforms and DB
   writes, where the GIL and blocking C calls (`psycopg2`) mean green threads
   don't add parallelism — they serialise. Adding compute (more vCPUs) does
   nothing for an eventlet worker pinned by the GIL.
2. **Its concurrency benefit was never even used.** `CELERY_WORKER_CONCURRENCY`
   has never been set (defaults to `1` via `${CELERY_WORKER_CONCURRENCY:-1}`),
   so the worker only ever ran one task at a time. There was no throughput to
   lose by moving off eventlet.
3. **No enforced timeouts.** eventlet does not enforce Celery's
   `task_time_limit` (it has no supervisor process to kill a running task), so
   the configured 30-minute limit never fired — a liability that gets worse the
   more concurrency we add.

## Decision

Run the worker with the **prefork** pool (`-P prefork`), keeping
`--concurrency ${CELERY_WORKER_CONCURRENCY:-1}`.

Prefork forks a pool of child processes under a MainProcess supervisor. This is
the model that lets us scale on this workload:

- **Compute → throughput.** Each child is a real OS process, so raising
  `--concurrency` (and the ECS task's vCPU/memory) converts directly into
  parallel task execution, GIL-free across processes. This is the lever for
  "more powerful async ECS tasks".
- **Bounded tasks.** The MainProcess enforces the time limit and `SIGKILL`s a
  child on hard-limit — even one blocked inside a C call like `psycopg2`. Safe
  ceilings are a prerequisite for turning concurrency up.

Concurrency stays at `1` for now (no behavioural change), with headroom to
increase deliberately (see roadmap).

## Consequences

- Concurrency is passed explicitly, avoiding the Fargate pitfall where prefork
  would otherwise fork one child per *host* CPU (`os.cpu_count()` reports the
  host, not the task's vCPU allocation) and risk OOM.
- Trade-off vs eventlet: prefork children are heavier (a full process each), so
  concurrency is bounded by ECS task memory, not cheap like green threads. This
  is acceptable because the workload isn't the massively-concurrent-I/O shape
  eventlet optimises for.

## Roadmap (scaling levers, deliberate follow-ups)

- **Scale up:** increase `CELERY_WORKER_CONCURRENCY` (e.g. to match vCPUs) and
  the ECS task's `cpu`/`memory`, gated on measuring per-child memory against the
  task limit to avoid trading a hang for OOM kills.
- **Scale out:** increase ECS `desired_count` for more independent workers/SQS
  consumers — lower per-task-memory risk than raising concurrency, at more cost.
