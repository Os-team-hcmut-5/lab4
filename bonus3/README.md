# Bonus 3 — Cron-Like Periodic Jobs

## Overview

This bonus extends the base scheduler (`scheduler.c`) with a **cron thread**
that automatically injects periodic background jobs into the queue at fixed
simulated-time intervals.  Three built-in rules are provided (configurable via
CSV):

| Rule | Interval | Runtime | Priority | Description |
|------|----------|---------|----------|-------------|
| `daily_report` | every 20 t.u. | 3 | 2 | Generate daily seller report |
| `temp_cleanup` | every 15 t.u. | 1 | 3 | Delete temporary image files |
| `failed_retry` | every 10 t.u. | 2 | 1 | Re-queue previously failed jobs |

Cron-injected jobs are marked `[CRON]` in the log and tracked separately in
the final summary.

## File Structure

```
bonus_3/
├── scheduler_cron.c      ← main source (extends scheduler.c)
├── cron_config.csv       ← cron rule definitions
├── workloads/
│   └── workload_cron.csv ← sample job workload
└── README.md
```

## Build

```bash
gcc -Wall -Wextra -O2 -pthread scheduler_cron.c -o scheduler_cron
```

## Run

```bash
# Using built-in default cron rules (3 rules hard-coded)
./scheduler_cron ../workloads/workload_a.csv fifo 4
./scheduler_cron ../workloads/workload_b.csv sjf 4
./scheduler_cron ../workloads/workload_c.csv priority 4

# Using a custom cron config file
./scheduler_cron workloads/workload_cron.csv priority 4 cron_config.csv
```

Supported policies: `fifo`, `sjf`, `priority`