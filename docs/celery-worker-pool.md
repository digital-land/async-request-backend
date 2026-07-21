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
   process). `psycopg2` can cooperate with the eventlet hub when greening is
   enabled (e.g. `psycogreen`), but that isn't configured here. Either way,
   eventlet runs green threads in a single OS thread, so the CPU-bound transforms
   these tasks spend most of their time in don't execute in parallel — extra
   vCPUs do little for an eventlet worker on this workload.
2. **Its concurrency benefit was never even used.** `CELERY_WORKER_CONCURRENCY`
   has never been set (defaults to `1` via `${CELERY_WORKER_CONCURRENCY:-1}`),
   so the worker only ever ran one task at a time. There was no throughput to
   lose by moving off eventlet.
3. **No enforced timeouts.** On the deployed versions (Celery 5.3.6, eventlet
   0.35.2), the eventlet pool did not enforce `task_time_limit`: in local
   testing a blocking task ran well past a short limit with no supervisor
   process to interrupt it, so the configured 30-minute limit did not fire — a
   liability that gets worse the more concurrency we add.

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

Configured task concurrency stays at `1` for now, with headroom to increase it
deliberately (see roadmap).

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
