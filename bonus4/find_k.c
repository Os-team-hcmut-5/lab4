#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define RESULT_DIR "lab4/bonus4/worker_results"
#define CSV_OUT RESULT_DIR "/find_k_results.csv"
#define EXTRA_CSV_OUT RESULT_DIR "/find_k_extra_workers.csv"

#define DATASET_COUNT 3
#define POLICY_COUNT 3
#define EXTRA_COUNT 8

typedef enum {
    POLICY_FIFO,
    POLICY_SJF,
    POLICY_PRIORITY
} Policy;

typedef struct {
    int job_id;
    char seller_id[32];
    int arrival_time;
    int estimated_runtime;
    int priority;
    char job_type[64];
    int start_time;
    int finish_time;
    int done;
} Job;

typedef struct {
    const char *name;
    const char *path;
} Dataset;

typedef struct {
    char dataset[32];
    char policy[32];
    int workers;
    int total_jobs;
    int total_simulation_time;
    double average_waiting_time;
    double average_turnaround_time;
    double throughput;
    double worker_utilization;
    int starvation_risk_jobs;
} Result;

typedef struct {
    int time;
    int delta;
} Event;

static const Dataset datasets[DATASET_COUNT] = {
    {"jobs_100", "lab4/jobs_100.csv"},
    {"jobs_1000", "lab4/jobs_1000.csv"},
    {"jobs_10000", "lab4/jobs_10000.csv"},
};

static const char *policy_names[POLICY_COUNT] = {"FIFO", "SJF", "Priority"};
static const int extra_offsets[EXTRA_COUNT] = {0, 1, 5, 10, 20, 50, 100, 200};

static int directory_exists(const char *path) {
    struct stat st;
    return stat(path, &st) == 0 && S_ISDIR(st.st_mode);
}

static int ensure_dir(const char *path) {
    if (directory_exists(path)) return 1;
    if (mkdir(path, 0775) != 0 && errno != EEXIST) {
        perror(path);
        return 0;
    }
    return 1;
}

static int move_to_project_root(void) {
    if (access("lab4/bonus4", F_OK) == 0) return 1;
    if (chdir("../..") == 0 && access("lab4/bonus4", F_OK) == 0) return 1;
    fprintf(stderr, "Could not find project root. Run from LAB4 root or lab4/bonus4.\n");
    return 0;
}

static int ensure_result_dir(void) {
    return ensure_dir("lab4") &&
           ensure_dir("lab4/bonus4") &&
           ensure_dir(RESULT_DIR);
}

static int compare_by_arrival(const void *a, const void *b) {
    const Job *job_a = (const Job *)a;
    const Job *job_b = (const Job *)b;
    if (job_a->arrival_time != job_b->arrival_time) {
        return job_a->arrival_time - job_b->arrival_time;
    }
    return job_a->job_id - job_b->job_id;
}

static int compare_events(const void *a, const void *b) {
    const Event *event_a = (const Event *)a;
    const Event *event_b = (const Event *)b;
    if (event_a->time != event_b->time) {
        return event_a->time - event_b->time;
    }
    return event_a->delta - event_b->delta;
}

static int load_jobs(const char *path, Job **jobs_out, int *count_out) {
    FILE *file = fopen(path, "r");
    char line[256];
    int capacity = 1024;
    int count = 0;
    Job *jobs = malloc((size_t)capacity * sizeof(Job));

    if (!file) {
        perror(path);
        free(jobs);
        return 0;
    }
    if (!jobs) {
        fprintf(stderr, "Memory allocation failed\n");
        fclose(file);
        return 0;
    }

    if (!fgets(line, sizeof(line), file)) {
        fprintf(stderr, "%s is empty\n", path);
        fclose(file);
        free(jobs);
        return 0;
    }

    while (fgets(line, sizeof(line), file)) {
        Job job;
        line[strcspn(line, "\r\n")] = '\0';
        if (line[0] == '\0') continue;

        memset(&job, 0, sizeof(job));
        if (sscanf(line, "%d,%31[^,],%d,%d,%d,%63[^\n]",
                   &job.job_id,
                   job.seller_id,
                   &job.arrival_time,
                   &job.estimated_runtime,
                   &job.priority,
                   job.job_type) != 6) {
            continue;
        }

        if (count >= capacity) {
            capacity *= 2;
            Job *grown = realloc(jobs, (size_t)capacity * sizeof(Job));
            if (!grown) {
                fprintf(stderr, "Memory reallocation failed\n");
                fclose(file);
                free(jobs);
                return 0;
            }
            jobs = grown;
        }
        jobs[count++] = job;
    }

    fclose(file);
    qsort(jobs, (size_t)count, sizeof(Job), compare_by_arrival);

    *jobs_out = jobs;
    *count_out = count;
    return 1;
}

static int compute_converged_k(Job *jobs, int count) {
    Event *events = malloc((size_t)count * 2 * sizeof(Event));
    int active = 0;
    int max_active = 0;

    if (!events) {
        fprintf(stderr, "Memory allocation failed\n");
        return 0;
    }

    for (int i = 0; i < count; i++) {
        events[i * 2].time = jobs[i].arrival_time;
        events[i * 2].delta = 1;
        events[i * 2 + 1].time = jobs[i].arrival_time + jobs[i].estimated_runtime;
        events[i * 2 + 1].delta = -1;
    }

    qsort(events, (size_t)count * 2, sizeof(Event), compare_events);

    for (int i = 0; i < count * 2; i++) {
        active += events[i].delta;
        if (active > max_active) max_active = active;
    }

    free(events);
    return max_active;
}

static void converged_result(const Dataset *dataset, Job *jobs, int count, Policy policy,
                             int workers, Result *result) {
    long total_runtime = 0;
    int max_finish_time = 0;

    for (int i = 0; i < count; i++) {
        int finish_time = jobs[i].arrival_time + jobs[i].estimated_runtime;
        total_runtime += jobs[i].estimated_runtime;
        if (finish_time > max_finish_time) max_finish_time = finish_time;
    }

    memset(result, 0, sizeof(*result));
    snprintf(result->dataset, sizeof(result->dataset), "%s", dataset->name);
    snprintf(result->policy, sizeof(result->policy), "%s", policy_names[policy]);
    result->workers = workers;
    result->total_jobs = count;
    result->total_simulation_time = max_finish_time;
    result->average_waiting_time = 0.0;
    result->average_turnaround_time = (double)total_runtime / count;
    result->throughput = (double)count / max_finish_time;
    result->worker_utilization = ((double)total_runtime / (workers * max_finish_time)) * 100.0;
    result->starvation_risk_jobs = 0;
}

static int write_csv(const Result *results, int count) {
    FILE *file = fopen(CSV_OUT, "w");
    if (!file) {
        perror(CSV_OUT);
        return 0;
    }

    fprintf(file, "dataset,policy,k_workers,total_jobs,total_simulation_time,average_waiting_time,");
    fprintf(file, "average_turnaround_time,throughput,worker_utilization,starvation_risk_jobs\n");

    for (int i = 0; i < count; i++) {
        fprintf(file, "%s,%s,%d,%d,%d,%.2f,%.2f,%.3f,%.2f,%d\n",
                results[i].dataset,
                results[i].policy,
                results[i].workers,
                results[i].total_jobs,
                results[i].total_simulation_time,
                results[i].average_waiting_time,
                results[i].average_turnaround_time,
                results[i].throughput,
                results[i].worker_utilization,
                results[i].starvation_risk_jobs);
    }

    fclose(file);
    return 1;
}

static int write_extra_csv(const Result *results, int count) {
    FILE *file = fopen(EXTRA_CSV_OUT, "w");
    if (!file) {
        perror(EXTRA_CSV_OUT);
        return 0;
    }

    fprintf(file, "dataset,policy,workers,total_jobs,total_simulation_time,average_waiting_time,");
    fprintf(file, "average_turnaround_time,throughput,worker_utilization,starvation_risk_jobs\n");

    for (int i = 0; i < count; i++) {
        fprintf(file, "%s,%s,%d,%d,%d,%.2f,%.2f,%.3f,%.2f,%d\n",
                results[i].dataset,
                results[i].policy,
                results[i].workers,
                results[i].total_jobs,
                results[i].total_simulation_time,
                results[i].average_waiting_time,
                results[i].average_turnaround_time,
                results[i].throughput,
                results[i].worker_utilization,
                results[i].starvation_risk_jobs);
    }

    fclose(file);
    return 1;
}

static void print_results(const Result *results, int count) {
    printf("\nConvergence criterion: average waiting time = 0 and starvation-risk jobs = 0.\n");
    printf("At this k, adding more workers cannot improve the simulated schedule further.\n\n");
    printf("%-11s %-9s %8s %8s %10s %10s %11s %9s %10s\n",
           "Dataset", "Policy", "k", "Time", "AvgWait", "AvgTurn", "Through", "Util(%)", "Starve");
    printf("------------------------------------------------------------------------------------------------\n");

    for (int i = 0; i < count; i++) {
        printf("%-11s %-9s %8d %8d %10.2f %10.2f %11.3f %9.2f %10d\n",
               results[i].dataset,
               results[i].policy,
               results[i].workers,
               results[i].total_simulation_time,
               results[i].average_waiting_time,
               results[i].average_turnaround_time,
               results[i].throughput,
               results[i].worker_utilization,
               results[i].starvation_risk_jobs);
    }
}

int main(void) {
    Result results[DATASET_COUNT * POLICY_COUNT];
    Result extra_results[DATASET_COUNT * POLICY_COUNT * EXTRA_COUNT];
    int result_count = 0;
    int extra_count = 0;
    int global_k = 0;

    if (!move_to_project_root() || !ensure_result_dir()) {
        return 1;
    }

    for (int d = 0; d < DATASET_COUNT; d++) {
        Job *jobs = NULL;
        int count = 0;
        int k = 0;

        printf("Loading %s...\n", datasets[d].path);
        if (!load_jobs(datasets[d].path, &jobs, &count)) {
            return 1;
        }

        k = compute_converged_k(jobs, count);
        if (k == 0) {
            free(jobs);
            return 1;
        }

        for (int p = 0; p < POLICY_COUNT; p++) {
            Result result;
            printf("Converged k for %-10s policy=%s is %d\n", datasets[d].name, policy_names[p], k);

            if (k > global_k) global_k = k;
            converged_result(&datasets[d], jobs, count, (Policy)p, k, &result);
            results[result_count++] = result;

            for (int e = 0; e < EXTRA_COUNT; e++) {
                int workers = k + extra_offsets[e];
                converged_result(&datasets[d], jobs, count, (Policy)p, workers, &extra_results[extra_count++]);
            }
        }

        free(jobs);
    }

    if (!write_csv(results, result_count) || !write_extra_csv(extra_results, extra_count)) {
        return 1;
    }

    print_results(results, result_count);
    printf("\nSmallest k that satisfies all datasets and all policies: %d workers\n", global_k);
    printf("CSV saved to: %s\n", CSV_OUT);
    printf("Extra-worker check saved to: %s\n", EXTRA_CSV_OUT);

    return 0;
}
