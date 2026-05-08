# Multithreaded Job Scheduler Simulation

A multithreaded job scheduling simulator written in C using POSIX Threads (`pthread`).  
The program simulates how workers process jobs under different scheduling policies:

- FIFO (First In First Out)
- SJF (Shortest Job First)
- Priority Scheduling

---

## Features

- Multithreaded worker pool using `pthread`
- Simulated clock thread
- CSV-based job input
- Multiple scheduling algorithms
- Runtime statistics:
  - Average waiting time
  - Average turnaround time
  - Throughput
  - Worker utilization
  - Starvation risk detection

---

## File Structure

```text
.
├── scheduler.c
├── wa.csv
├── wb.csv
├── wc.csv
└── README.md
```

---

## CSV Input Format

The CSV file must contain the following columns:

```csv
job_id,seller_id,arrival_time,estimated_runtime,priority,job_type
```

### Example

```csv
job_id,seller_id,arrival_time,estimated_runtime,priority,job_type
1,A,0,5,2,Electronics
2,B,1,3,1,Fashion
3,C,2,7,3,Books
```

---

# Compile

Use GCC with pthread support:

```bash
gcc -Wall -pthread -o scheduler scheduler.c
```

---

# Run

```bash
./scheduler <csv_file> <policy> <workers>
```

## Syntax Example

```bash
./scheduler wa.csv fifo 4
```

---


# Scheduling Policies

| Policy Argument | Description |
|---|---|
| `fifo` | First In First Out |
| `sjf` | Shortest Job First |
| `priority` | Higher priority job first (smaller number = higher priority) |

---

# Bonus tasks
## Aging priority Scheduling
Strict Priority Scheduling often suffers from the **Starvation** problem, where low-priority jobs are indefinitely deferred if a continuous stream of high-priority jobs arrives. To mitigate this, we implemented an **Aging Priority Scheduling** algorithm.
```bash
cd bonus
gcc -Wall -pthread -o aging_scheduler aging_priority_scheduling.c
```
# Run
```bash
./aging_scheduler ../<csv_file> priority <number of workers>
```

# Requirements

- GCC compiler
- POSIX Threads (`pthread`)

---

# Notes

- One simulation tick = `0.1 seconds`
- Maximum supported jobs: `1000`
- Worker utilization is calculated based on total busy time
- Starvation risk is detected when waiting time exceeds:

```text
2 × average runtime
```

---

# Clean Build

Remove executable:

```bash
rm scheduler
```

---