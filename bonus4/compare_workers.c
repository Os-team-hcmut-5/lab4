#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define RESULT_DIR "lab4/bonus4/worker_results"
#define CSV_OUT RESULT_DIR "/worker_dataset_comparison.csv"
#define SVG_OUT RESULT_DIR "/worker_dataset_comparison.svg"

#define DATASET_COUNT 3
#define WORKER_COUNT 3
#define POLICY_COUNT 3
#define METRIC_COUNT 6
#define MAX_ROWS (DATASET_COUNT * WORKER_COUNT * POLICY_COUNT)

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
    const char *field;
    const char *title;
    const char *hint;
} MetricInfo;

static const Dataset datasets[DATASET_COUNT] = {
    {"jobs_100", "lab4/jobs_100.csv"},
    {"jobs_1000", "lab4/jobs_1000.csv"},
    {"jobs_10000", "lab4/jobs_10000.csv"},
};

static const int workers_to_test[WORKER_COUNT] = {2, 4, 8};
static const char *policy_names[POLICY_COUNT] = {"FIFO", "SJF", "Priority"};
static const char *policy_colors[POLICY_COUNT] = {"#2f6f9f", "#4f8f5f", "#c4663a"};

static const MetricInfo metrics[METRIC_COUNT] = {
    {"total_simulation_time", "Total simulation time", "lower is better"},
    {"average_waiting_time", "Average waiting time", "lower is better"},
    {"average_turnaround_time", "Average turnaround time", "lower is better"},
    {"throughput", "Throughput", "higher is better"},
    {"worker_utilization", "Worker utilization (%)", "higher is better"},
    {"starvation_risk_jobs", "Starvation-risk jobs", "lower is better"},
};

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

static int choose_job(Job *jobs, int count, Policy policy, int now) {
    int best = -1;

    for (int i = 0; i < count; i++) {
        if (jobs[i].done || jobs[i].arrival_time > now) continue;

        if (best == -1) {
            best = i;
            continue;
        }

        if (policy == POLICY_FIFO) {
            if (jobs[i].arrival_time < jobs[best].arrival_time) best = i;
        } else if (policy == POLICY_SJF) {
            if (jobs[i].estimated_runtime < jobs[best].estimated_runtime) best = i;
        } else {
            if (jobs[i].priority > jobs[best].priority) best = i;
        }
    }

    return best;
}

static int next_arrival(Job *jobs, int count, int now) {
    int next = INT_MAX;
    for (int i = 0; i < count; i++) {
        if (!jobs[i].done && jobs[i].arrival_time > now && jobs[i].arrival_time < next) {
            next = jobs[i].arrival_time;
        }
    }
    return next;
}

static int earliest_worker(const int *worker_available, int workers) {
    int best = 0;
    for (int i = 1; i < workers; i++) {
        if (worker_available[i] < worker_available[best]) best = i;
    }
    return best;
}

static int simulate_dataset(const Dataset *dataset, Job *base_jobs, int count, Policy policy,
                            int workers, Result *result) {
    Job *jobs = malloc((size_t)count * sizeof(Job));
    int *worker_available = calloc((size_t)workers, sizeof(int));
    long total_runtime = 0;
    int completed = 0;

    if (!jobs || !worker_available) {
        fprintf(stderr, "Memory allocation failed\n");
        free(jobs);
        free(worker_available);
        return 0;
    }

    memcpy(jobs, base_jobs, (size_t)count * sizeof(Job));
    for (int i = 0; i < count; i++) {
        jobs[i].start_time = -1;
        jobs[i].finish_time = -1;
        jobs[i].done = 0;
        total_runtime += jobs[i].estimated_runtime;
    }

    while (completed < count) {
        int worker = earliest_worker(worker_available, workers);
        int now = worker_available[worker];
        int job_index = choose_job(jobs, count, policy, now);

        if (job_index == -1) {
            int arrival = next_arrival(jobs, count, now);
            if (arrival == INT_MAX) break;
            worker_available[worker] = arrival;
            continue;
        }

        jobs[job_index].start_time = now;
        jobs[job_index].finish_time = now + jobs[job_index].estimated_runtime;
        jobs[job_index].done = 1;
        worker_available[worker] = jobs[job_index].finish_time;
        completed++;
    }

    int max_finish_time = 0;
    long total_waiting_time = 0;
    long total_turnaround_time = 0;
    double avg_runtime = (double)total_runtime / count;
    double starvation_threshold = 2.0 * avg_runtime;
    int starvation_count = 0;

    for (int i = 0; i < count; i++) {
        int waiting_time = jobs[i].start_time - jobs[i].arrival_time;
        int turnaround_time = jobs[i].finish_time - jobs[i].arrival_time;
        total_waiting_time += waiting_time;
        total_turnaround_time += turnaround_time;
        if (jobs[i].finish_time > max_finish_time) max_finish_time = jobs[i].finish_time;
        if (waiting_time > starvation_threshold) starvation_count++;
    }

    memset(result, 0, sizeof(*result));
    snprintf(result->dataset, sizeof(result->dataset), "%s", dataset->name);
    snprintf(result->policy, sizeof(result->policy), "%s", policy_names[policy]);
    result->workers = workers;
    result->total_jobs = count;
    result->total_simulation_time = max_finish_time;
    result->average_waiting_time = (double)total_waiting_time / count;
    result->average_turnaround_time = (double)total_turnaround_time / count;
    result->throughput = (double)count / max_finish_time;
    result->worker_utilization = ((double)total_runtime / (workers * max_finish_time)) * 100.0;
    result->starvation_risk_jobs = starvation_count;

    free(jobs);
    free(worker_available);
    return 1;
}

static int write_csv(const Result *results, int count) {
    FILE *file = fopen(CSV_OUT, "w");
    if (!file) {
        perror(CSV_OUT);
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

static double metric_value(const Result *result, const char *field) {
    if (strcmp(field, "total_simulation_time") == 0) return result->total_simulation_time;
    if (strcmp(field, "average_waiting_time") == 0) return result->average_waiting_time;
    if (strcmp(field, "average_turnaround_time") == 0) return result->average_turnaround_time;
    if (strcmp(field, "throughput") == 0) return result->throughput;
    if (strcmp(field, "worker_utilization") == 0) return result->worker_utilization;
    if (strcmp(field, "starvation_risk_jobs") == 0) return result->starvation_risk_jobs;
    return 0.0;
}

static const Result *find_result(const Result *results, int count, const char *dataset,
                                 const char *policy, int workers) {
    for (int i = 0; i < count; i++) {
        if (strcmp(results[i].dataset, dataset) == 0 &&
            strcmp(results[i].policy, policy) == 0 &&
            results[i].workers == workers) {
            return &results[i];
        }
    }
    return NULL;
}

static double x_for_worker(int workers, double left, double plot_width) {
    return left + ((double)(workers - 2) / 6.0) * plot_width;
}

static double y_for_value(double value, double min_value, double max_value, double top, double plot_height) {
    if (max_value <= min_value) return top + plot_height / 2.0;
    return top + plot_height - ((value - min_value) / (max_value - min_value)) * plot_height;
}

static int write_dataset_svg_page(FILE *file, const Result *results, int count, const Dataset *dataset,
                                  int page_y) {
    const int card_w = 455;
    const int card_h = 300;
    const int margin_x = 35;
    const int gap_x = 25;
    const int gap_y = 45;

    fprintf(file, "<text x=\"%d\" y=\"%d\" font-family=\"Arial\" font-size=\"24\" font-weight=\"700\" fill=\"#1f2933\">%s</text>\n",
            margin_x, page_y + 38, dataset->name);

    for (int m = 0; m < METRIC_COUNT; m++) {
        int col = m % 3;
        int row = m / 3;
        double card_x = margin_x + col * (card_w + gap_x);
        double card_y = page_y + 65 + row * (card_h + gap_y);
        double left = card_x + 60;
        double top = card_y + 65;
        double plot_w = card_w - 95;
        double plot_h = card_h - 115;
        double min_value = 1e100;
        double max_value = -1e100;

        for (int p = 0; p < POLICY_COUNT; p++) {
            for (int w = 0; w < WORKER_COUNT; w++) {
                const Result *result = find_result(results, count, dataset->name, policy_names[p], workers_to_test[w]);
                if (!result) continue;
                double value = metric_value(result, metrics[m].field);
                if (value < min_value) min_value = value;
                if (value > max_value) max_value = value;
            }
        }

        if (min_value > 0.0) min_value *= 0.92;
        if (max_value > 0.0) max_value *= 1.08;

        fprintf(file, "<rect x=\"%.0f\" y=\"%.0f\" width=\"%d\" height=\"%d\" rx=\"8\" fill=\"#ffffff\" stroke=\"#d9e2ec\"/>\n",
                card_x, card_y, card_w, card_h);
        fprintf(file, "<text x=\"%.0f\" y=\"%.0f\" font-family=\"Arial\" font-size=\"16\" font-weight=\"700\" fill=\"#1f2933\">%s</text>\n",
                card_x + 22, card_y + 28, metrics[m].title);
        fprintf(file, "<text x=\"%.0f\" y=\"%.0f\" font-family=\"Arial\" font-size=\"12\" fill=\"#7b8794\">%s</text>\n",
                card_x + 22, card_y + 47, metrics[m].hint);

        fprintf(file, "<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#9fb3c8\"/>\n",
                left, top + plot_h, left + plot_w, top + plot_h);
        fprintf(file, "<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#9fb3c8\"/>\n",
                left, top, left, top + plot_h);

        for (int tick = 0; tick < WORKER_COUNT; tick++) {
            double x = x_for_worker(workers_to_test[tick], left, plot_w);
            fprintf(file, "<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#d9e2ec\"/>\n",
                    x, top, x, top + plot_h);
            fprintf(file, "<text x=\"%.1f\" y=\"%.1f\" font-family=\"Arial\" font-size=\"12\" text-anchor=\"middle\" fill=\"#52606d\">%d</text>\n",
                    x, top + plot_h + 20, workers_to_test[tick]);
        }

        for (int tick = 0; tick <= 4; tick++) {
            double y = top + (plot_h / 4.0) * tick;
            double value = max_value - ((max_value - min_value) / 4.0) * tick;
            fprintf(file, "<line x1=\"%.1f\" y1=\"%.1f\" x2=\"%.1f\" y2=\"%.1f\" stroke=\"#eef2f7\"/>\n",
                    left, y, left + plot_w, y);
            fprintf(file, "<text x=\"%.1f\" y=\"%.1f\" font-family=\"Arial\" font-size=\"11\" text-anchor=\"end\" fill=\"#52606d\">%.1f</text>\n",
                    left - 8, y + 4, value);
        }

        for (int p = 0; p < POLICY_COUNT; p++) {
            fprintf(file, "<polyline fill=\"none\" stroke=\"%s\" stroke-width=\"3\" points=\"", policy_colors[p]);
            for (int w = 0; w < WORKER_COUNT; w++) {
                const Result *result = find_result(results, count, dataset->name, policy_names[p], workers_to_test[w]);
                if (!result) continue;
                fprintf(file, "%.1f,%.1f ",
                        x_for_worker(workers_to_test[w], left, plot_w),
                        y_for_value(metric_value(result, metrics[m].field), min_value, max_value, top, plot_h));
            }
            fprintf(file, "\"/>\n");

            for (int w = 0; w < WORKER_COUNT; w++) {
                const Result *result = find_result(results, count, dataset->name, policy_names[p], workers_to_test[w]);
                if (!result) continue;
                fprintf(file, "<circle cx=\"%.1f\" cy=\"%.1f\" r=\"4.8\" fill=\"%s\" stroke=\"#ffffff\" stroke-width=\"1.5\"/>\n",
                        x_for_worker(workers_to_test[w], left, plot_w),
                        y_for_value(metric_value(result, metrics[m].field), min_value, max_value, top, plot_h),
                        policy_colors[p]);
            }
        }
    }

    return 1;
}

static int write_svg(const Result *results, int count) {
    const int width = 1500;
    const int page_h = 820;
    const int height = page_h * DATASET_COUNT + 80;
    FILE *file = fopen(SVG_OUT, "w");
    if (!file) {
        perror(SVG_OUT);
        return 0;
    }

    fprintf(file, "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%d\" height=\"%d\" viewBox=\"0 0 %d %d\">\n",
            width, height, width, height);
    fprintf(file, "<rect width=\"100%%\" height=\"100%%\" fill=\"#f7f8fa\"/>\n");
    fprintf(file, "<text x=\"35\" y=\"38\" font-family=\"Arial\" font-size=\"26\" font-weight=\"700\" fill=\"#1f2933\">Scheduler comparison: jobs_100, jobs_1000, jobs_10000</text>\n");
    fprintf(file, "<text x=\"35\" y=\"64\" font-family=\"Arial\" font-size=\"14\" fill=\"#52606d\">Analytical simulation in compare_workers.c, workers = 2, 4, 8</text>\n");

    for (int i = 0; i < POLICY_COUNT; i++) {
        int x = 980 + i * 135;
        fprintf(file, "<circle cx=\"%d\" cy=\"44\" r=\"6\" fill=\"%s\"/>\n", x, policy_colors[i]);
        fprintf(file, "<text x=\"%d\" y=\"49\" font-family=\"Arial\" font-size=\"14\" fill=\"#323f4b\">%s</text>\n", x + 12, policy_names[i]);
    }

    for (int d = 0; d < DATASET_COUNT; d++) {
        write_dataset_svg_page(file, results, count, &datasets[d], 80 + d * page_h);
    }

    fprintf(file, "</svg>\n");
    fclose(file);
    return 1;
}

static void print_results(const Result *results, int count) {
    printf("\n%-11s %-9s %7s %8s %11s %11s %11s %9s %10s\n",
           "Dataset", "Policy", "Workers", "Time", "AvgWait", "AvgTurn", "Through", "Util(%)", "Starve");
    printf("------------------------------------------------------------------------------------------------\n");
    for (int i = 0; i < count; i++) {
        printf("%-11s %-9s %7d %8d %11.2f %11.2f %11.3f %9.2f %10d\n",
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
    Result results[MAX_ROWS];
    int result_count = 0;

    if (!move_to_project_root() || !ensure_result_dir()) {
        return 1;
    }

    for (int d = 0; d < DATASET_COUNT; d++) {
        Job *jobs = NULL;
        int count = 0;

        printf("Loading %s...\n", datasets[d].path);
        if (!load_jobs(datasets[d].path, &jobs, &count)) {
            return 1;
        }

        for (int w = 0; w < WORKER_COUNT; w++) {
            for (int p = 0; p < POLICY_COUNT; p++) {
                printf("Simulating %-10s workers=%d policy=%s\n",
                       datasets[d].name, workers_to_test[w], policy_names[p]);
                if (result_count >= MAX_ROWS ||
                    !simulate_dataset(&datasets[d], jobs, count, (Policy)p,
                                      workers_to_test[w], &results[result_count])) {
                    free(jobs);
                    return 1;
                }
                result_count++;
            }
        }

        free(jobs);
    }

    if (!write_csv(results, result_count) || !write_svg(results, result_count)) {
        return 1;
    }

    print_results(results, result_count);
    printf("\nCSV saved to: %s\n", CSV_OUT);
    printf("Graph saved to: %s\n", SVG_OUT);

    return 0;
}
