
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>
#include <time.h>

#define MAX_JOBS       2000   
#define MAX_CRON_RULES  16
#define TICK_US        100000 

typedef enum { JOB_WAITING, JOB_RUNNING, JOB_DONE } job_status_t;

typedef struct {
    int           job_id;
    char          seller_id[32];
    int           arrival_time;
    int           estimated_runtime;
    int           priority;
    char          job_type[64];
    int           start_time;
    int           finish_time;
    job_status_t  status;
    bool          is_periodic;  
} job_t;

typedef struct {
    int  worker_id;
    pthread_t thread;
    int  busy_time;
} worker_t;

typedef enum { POLICY_FIFO, POLICY_SJF, POLICY_PRIORITY } policy_t;

typedef struct {
    char rule_name[64];  
    int  interval;       
    int  job_runtime;    
    int  job_priority;    
    char job_type[64];    
    int  next_fire_time;  
    int  fire_count;  
} cron_rule_t;

job_t        jobs[MAX_JOBS];
int          total_jobs       = 0;  
int          initial_jobs     = 0;   
int          completed_jobs   = 0;
int          current_sim_time = 0;
policy_t     current_policy;
int          num_workers      = 0;

cron_rule_t  cron_rules[MAX_CRON_RULES];
int          num_cron_rules   = 0;
bool         cron_done        = false; 

static int   next_job_id;

pthread_mutex_t queue_mutex   = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t  job_available = PTHREAD_COND_INITIALIZER;


job_t* scheduler_get_next_job(policy_t policy);
void   dispatcher_assign_job(worker_t *worker, job_t *job);
void  *worker_loop(void *arg);
void  *clock_loop(void *arg);
void  *cron_loop(void *arg);
void   load_jobs(const char *filename);
void   load_cron_config(const char *filename);
void   setup_default_cron_rules(void);
void   inject_cron_job(cron_rule_t *rule);

job_t* scheduler_get_next_job(policy_t policy)
{
    job_t *best = NULL;

    for (int i = 0; i < total_jobs; i++) {
        if (jobs[i].status != JOB_WAITING) continue;
        if (jobs[i].arrival_time > current_sim_time) continue;

        if (best == NULL) {
            best = &jobs[i];
            continue;
        }
        if (policy == POLICY_FIFO) {
            if (jobs[i].arrival_time < best->arrival_time) best = &jobs[i];
        } else if (policy == POLICY_SJF) {
            if (jobs[i].estimated_runtime < best->estimated_runtime) best = &jobs[i];
        } else if (policy == POLICY_PRIORITY) {
            if (jobs[i].priority < best->priority) best = &jobs[i];
        }
    }
    return best;
}


void dispatcher_assign_job(worker_t *worker, job_t *job)
{
    const char *tag = job->is_periodic ? "[CRON]" : "      ";
    printf("[time=%d] %s Worker %d starts  Job %d  seller=%-6s  runtime=%d  priority=%d  type=%s\n",
           current_sim_time, tag, worker->worker_id,
           job->job_id, job->seller_id,
           job->estimated_runtime, job->priority, job->job_type);

    usleep((unsigned)(job->estimated_runtime * TICK_US));

    printf("[time=%d] %s Worker %d finishes Job %d\n",
           current_sim_time, tag, worker->worker_id, job->job_id);

    worker->busy_time += job->estimated_runtime;
}

void* worker_loop(void *arg)
{
    worker_t *worker = (worker_t *)arg;

    while (1) {
        pthread_mutex_lock(&queue_mutex);

        while (!cron_done && scheduler_get_next_job(current_policy) == NULL) {
            pthread_cond_wait(&job_available, &queue_mutex);
        }

        if (cron_done && scheduler_get_next_job(current_policy) == NULL) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }

        job_t *next = scheduler_get_next_job(current_policy);
        if (next == NULL) {
            pthread_mutex_unlock(&queue_mutex);
            continue;
        }

        next->status     = JOB_RUNNING;
        next->start_time = current_sim_time;
        pthread_mutex_unlock(&queue_mutex);

        dispatcher_assign_job(worker, next);

        pthread_mutex_lock(&queue_mutex);
        next->finish_time = next->start_time + next->estimated_runtime;
        next->status      = JOB_DONE;
        completed_jobs++;
        pthread_cond_broadcast(&job_available);
        pthread_mutex_unlock(&queue_mutex);
    }
    return NULL;
}

void* clock_loop(void *arg)
{
    (void)arg;
    while (!cron_done) {
        usleep(TICK_US);
        pthread_mutex_lock(&queue_mutex);
        current_sim_time++;
        pthread_cond_broadcast(&job_available);
        pthread_mutex_unlock(&queue_mutex);
    }
    return NULL;
}

void inject_cron_job(cron_rule_t *rule)
{
    if (total_jobs >= MAX_JOBS) {
        fprintf(stderr, "[CRON] Warning: job table full, skipping %s\n",
                rule->rule_name);
        return;
    }

    job_t *j        = &jobs[total_jobs];
    j->job_id        = next_job_id++;
    snprintf(j->seller_id, sizeof(j->seller_id), "CRON");
    j->arrival_time   = current_sim_time;
    j->estimated_runtime = rule->job_runtime;
    j->priority       = rule->job_priority;
    snprintf(j->job_type, sizeof(j->job_type), "%s", rule->job_type);
    j->start_time     = 0;
    j->finish_time    = 0;
    j->status         = JOB_WAITING;
    j->is_periodic    = true;
    total_jobs++;

    printf("[time=%d] [CRON] Injected job %d  rule='%s'  runtime=%d  priority=%d\n",
           current_sim_time, j->job_id, rule->rule_name,
           rule->job_runtime, rule->job_priority);
}


#define MAX_SIM_TIME 200  

void* cron_loop(void *arg)
{
    (void)arg;

    while (1) {
        usleep(TICK_US); 

        pthread_mutex_lock(&queue_mutex);

        for (int i = 0; i < num_cron_rules; i++) {
            cron_rule_t *r = &cron_rules[i];
            if (current_sim_time >= r->next_fire_time) {
                inject_cron_job(r);
                r->fire_count++;
                r->next_fire_time = current_sim_time + r->interval;
                pthread_cond_broadcast(&job_available);
            }
        }

        bool all_csv_done = (completed_jobs >= initial_jobs);

        bool each_rule_fired_once = true;
        for (int i = 0; i < num_cron_rules; i++) {
            if (cron_rules[i].fire_count == 0) {
                each_rule_fired_once = false;
                break;
            }
        }

        if ((all_csv_done && each_rule_fired_once) ||
             current_sim_time >= MAX_SIM_TIME) {
            if (completed_jobs >= total_jobs || current_sim_time >= MAX_SIM_TIME) {
                cron_done = true;
                pthread_cond_broadcast(&job_available);
                pthread_mutex_unlock(&queue_mutex);
                break;
            }
        }

        pthread_mutex_unlock(&queue_mutex);
    }
    return NULL;
}

void load_jobs(const char *filename)
{
    FILE *f = fopen(filename, "r");
    if (!f) { perror("Error opening CSV"); exit(1); }

    char line[256];
    if (!fgets(line, sizeof(line), f)) {
        fprintf(stderr, "CSV empty or missing header\n");
        fclose(f); exit(1);
    }

    while (fgets(line, sizeof(line), f)) {
        if (total_jobs >= MAX_JOBS) {
            fprintf(stderr, "Warning: job limit reached\n"); break;
        }
        job_t *j = &jobs[total_jobs];
        j->start_time  = 0; j->finish_time = 0;
        j->status      = JOB_WAITING;
        j->is_periodic = false;

        int parsed = sscanf(line, "%d,%31[^,],%d,%d,%d,%63[^\n]",
                            &j->job_id, j->seller_id,
                            &j->arrival_time, &j->estimated_runtime,
                            &j->priority, j->job_type);
        if (parsed < 6) {
            fprintf(stderr, "Skipping malformed line: %s", line);
            continue;
        }
        total_jobs++;
    }
    fclose(f);

    initial_jobs = total_jobs;
    next_job_id  = total_jobs + 1;  
}

void load_cron_config(const char *filename)
{
    FILE *f = fopen(filename, "r");
    if (!f) {
        fprintf(stderr, "Warning: cannot open cron config '%s', using defaults\n",
                filename);
        setup_default_cron_rules();
        return;
    }

    char line[256];
    fgets(line, sizeof(line), f);

    while (fgets(line, sizeof(line), f) && num_cron_rules < MAX_CRON_RULES) {
        cron_rule_t *r = &cron_rules[num_cron_rules];
        int parsed = sscanf(line, "%63[^,],%d,%d,%d,%63[^\n]",
                            r->rule_name, &r->interval,
                            &r->job_runtime, &r->job_priority, r->job_type);
        if (parsed < 5) {
            fprintf(stderr, "Skipping malformed cron line: %s", line);
            continue;
        }
        r->next_fire_time = r->interval; 
        r->fire_count     = 0;
        num_cron_rules++;
    }
    fclose(f);

    if (num_cron_rules == 0) {
        fprintf(stderr, "Cron config had no valid rules, using defaults\n");
        setup_default_cron_rules();
    }
}

void setup_default_cron_rules(void)
{
    cron_rule_t *r = &cron_rules[0];
    snprintf(r->rule_name, sizeof(r->rule_name), "daily_report");
    r->interval       = 20;
    r->job_runtime    = 3;
    r->job_priority   = 2;
    snprintf(r->job_type, sizeof(r->job_type), "daily_report");
    r->next_fire_time = 20;
    r->fire_count     = 0;

    r = &cron_rules[1];
    snprintf(r->rule_name, sizeof(r->rule_name), "temp_cleanup");
    r->interval       = 15;
    r->job_runtime    = 1;
    r->job_priority   = 3;
    snprintf(r->job_type, sizeof(r->job_type), "cleanup");
    r->next_fire_time = 15;
    r->fire_count     = 0;

    r = &cron_rules[2];
    snprintf(r->rule_name, sizeof(r->rule_name), "failed_retry");
    r->interval       = 10;
    r->job_runtime    = 2;
    r->job_priority   = 1;
    snprintf(r->job_type, sizeof(r->job_type), "retry");
    r->next_fire_time = 10;
    r->fire_count     = 0;

    num_cron_rules = 3;
}
int main(int argc, char *argv[])
{
    if (argc < 4 || argc > 5) {
        printf("Usage: %s <csv_file> <policy: fifo|sjf|priority> <workers> [cron_config.csv]\n",
               argv[0]);
        return 1;
    }

    load_jobs(argv[1]);

    if      (strcmp(argv[2], "fifo")     == 0) current_policy = POLICY_FIFO;
    else if (strcmp(argv[2], "sjf")      == 0) current_policy = POLICY_SJF;
    else if (strcmp(argv[2], "priority") == 0) current_policy = POLICY_PRIORITY;
    else { printf("Unknown policy. Use fifo, sjf, or priority.\n"); return 1; }

    num_workers = atoi(argv[3]);
    if (num_workers <= 0) {
        fprintf(stderr, "Error: workers must be a positive integer.\n"); return 1;
    }

    if (argc == 5) load_cron_config(argv[4]);
    else           setup_default_cron_rules();

    printf("\n=== Cron-Like Periodic Scheduler ===\n");
    printf("CSV jobs   : %d\n", initial_jobs);
    printf("Cron rules : %d\n", num_cron_rules);
    printf("Policy     : %s\n", argv[2]);
    printf("Workers    : %d\n\n", num_workers);

    for (int i = 0; i < num_cron_rules; i++) {
        printf("  [rule %d] %-20s  every=%d  runtime=%d  priority=%d\n",
               i, cron_rules[i].rule_name, cron_rules[i].interval,
               cron_rules[i].job_runtime, cron_rules[i].job_priority);
    }
    printf("\n");

    worker_t *workers = malloc((size_t)num_workers * sizeof(worker_t));
    if (!workers) { fprintf(stderr, "malloc failed\n"); return 1; }

    pthread_t clock_thread, cron_thread;
    pthread_create(&clock_thread, NULL, clock_loop, NULL);
    pthread_create(&cron_thread,  NULL, cron_loop,  NULL);

    for (int i = 0; i < num_workers; i++) {
        workers[i].worker_id = i;
        workers[i].busy_time = 0;
        pthread_create(&workers[i].thread, NULL, worker_loop, &workers[i]);
    }

    for (int i = 0; i < num_workers; i++)
        pthread_join(workers[i].thread, NULL);
    pthread_join(cron_thread,  NULL);
    pthread_join(clock_thread, NULL);

    int    total_waiting    = 0;
    int    total_turnaround = 0;
    int    total_busy       = 0;
    long   total_runtime    = 0;
    int    csv_waiting      = 0;
    int    csv_turnaround   = 0;
    int    cron_count       = 0;

    for (int i = 0; i < total_jobs; i++) {
        int w  = jobs[i].start_time  - jobs[i].arrival_time;
        int ta = jobs[i].finish_time - jobs[i].arrival_time;
        total_waiting    += w;
        total_turnaround += ta;
        total_runtime    += jobs[i].estimated_runtime;
        if (!jobs[i].is_periodic) { csv_waiting += w; csv_turnaround += ta; }
        else                        cron_count++;
    }
    for (int i = 0; i < num_workers; i++) total_busy += workers[i].busy_time;

    double avg_runtime  = (total_jobs > 0) ? (double)total_runtime / total_jobs : 0;
    int    stv_thresh   = (int)(2 * avg_runtime);
    int    stv_count    = 0;
    for (int i = 0; i < total_jobs; i++) {
        if ((jobs[i].start_time - jobs[i].arrival_time) > stv_thresh) stv_count++;
    }

    double avg_wait  = (total_jobs > 0) ? (double)total_waiting    / total_jobs : 0;
    double avg_ta    = (total_jobs > 0) ? (double)total_turnaround / total_jobs : 0;
    double throughput = (current_sim_time > 0)
                        ? (double)total_jobs / current_sim_time : 0;
    double util       = (current_sim_time > 0)
                        ? ((double)total_busy / ((double)num_workers * current_sim_time)) * 100.0
                        : 0;

    printf("\n--- Cron Rule Summary ---\n");
    for (int i = 0; i < num_cron_rules; i++) {
        printf("  %-20s  fired=%d times\n",
               cron_rules[i].rule_name, cron_rules[i].fire_count);
    }

    printf("\n--- Run Summary ---\n");
    printf("Policy                 : %s\n", argv[2]);
    printf("Workers                : %d\n", num_workers);
    printf("CSV jobs               : %d\n", initial_jobs);
    printf("Cron-injected jobs     : %d\n", cron_count);
    printf("Total jobs processed   : %d\n", total_jobs);
    printf("Total simulation time  : %d\n", current_sim_time);
    printf("Average waiting time   : %.2f\n", avg_wait);
    printf("Average turnaround time: %.2f\n", avg_ta);
    printf("Throughput             : %.3f jobs/unit time\n", throughput);
    printf("Worker utilization     : %.2f%%\n", util);
    printf("Starvation-risk jobs   : %d  (threshold=%d)\n", stv_count, stv_thresh);

    free(workers);
    return 0;
}