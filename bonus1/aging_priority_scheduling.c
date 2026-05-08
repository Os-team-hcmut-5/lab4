#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>

#define MAX_JOBS 1000
#define TICK_US 100000
#define AGING_RATE 15 

typedef enum {
    JOB_WAITING,
    JOB_RUNNING,
    JOB_DONE
} job_status_t;

typedef struct {
    int job_id;
    char seller_id[32];
    int arrival_time;
    int estimated_runtime;
    int priority;
    char job_type[64];
    int start_time;
    int finish_time;
    job_status_t status;
} job_t;

typedef struct {
    int worker_id;
    pthread_t thread;
    int busy_time; 
} worker_t;

typedef enum {
    POLICY_FIFO,
    POLICY_SJF,
    POLICY_PRIORITY
} policy_t;

job_t jobs[MAX_JOBS];
int total_jobs = 0;
int completed_jobs = 0;
int current_sim_time = 0;
policy_t current_policy;
int num_workers = 0;

pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t job_available = PTHREAD_COND_INITIALIZER;

job_t* scheduler_get_next_job(policy_t policy);
void dispatcher_assign_job(worker_t* worker, job_t* job);
void* worker_loop(void* arg);
void* clock_loop(void* arg);
void load_jobs(const char* filename);

job_t* scheduler_get_next_job(policy_t policy) {
    job_t* best_job = NULL;
    
    for (int i = 0; i < total_jobs; i++) {
        if (jobs[i].status == JOB_WAITING && jobs[i].arrival_time <= current_sim_time) {
            if (best_job == NULL) {
                best_job = &jobs[i];
            } else {
                if (policy == POLICY_FIFO) {
                    if (jobs[i].arrival_time < best_job->arrival_time) {
                        best_job = &jobs[i];
                    }
                } else if (policy == POLICY_SJF) {
                    if (jobs[i].estimated_runtime < best_job->estimated_runtime) {
                        best_job = &jobs[i];
                    }
                } else if (policy == POLICY_PRIORITY) {
                    int wait_time_i = current_sim_time - jobs[i].arrival_time;
                    int effective_priority_i = jobs[i].priority - (wait_time_i / AGING_RATE);
                    if (effective_priority_i < 1) effective_priority_i = 1;

                    int wait_time_best = current_sim_time - best_job->arrival_time;
                    int effective_priority_best = best_job->priority - (wait_time_best / AGING_RATE);
                    if (effective_priority_best < 1) effective_priority_best = 1;

                    if (effective_priority_i < effective_priority_best) {
                        best_job = &jobs[i];
                    } 
                    else if (effective_priority_i == effective_priority_best) {
                        if (jobs[i].arrival_time < best_job->arrival_time) {
                            best_job = &jobs[i];
                        }
                    }
                }
            }
        }
    }
    return best_job;
}

void dispatcher_assign_job(worker_t* worker, job_t* job) {
    int current_wait = current_sim_time - job->arrival_time;
    int eff_prio = job->priority - (current_wait / AGING_RATE);
    if (eff_prio < 1) eff_prio = 1;

    printf("[time=%d] Worker %d starts Job %d seller=%s runtime=%d base_priority=%d effective_priority=%d\n", 
           current_sim_time, worker->worker_id, job->job_id, job->seller_id, 
           job->estimated_runtime, job->priority, eff_prio);
           
    usleep(job->estimated_runtime * TICK_US);
    
    // Đã sửa lỗi hiển thị log gấp đôi thời gian
    printf("[time=%d] Worker %d finishes Job %d\n", 
           job->start_time + job->estimated_runtime, worker->worker_id, job->job_id);
           
    worker->busy_time += job->estimated_runtime;
}

void* worker_loop(void* arg) {
    worker_t* worker = (worker_t*)arg;
    
    while (1) {
        pthread_mutex_lock(&queue_mutex);
        
        if (completed_jobs >= total_jobs) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        
        job_t* next_job = scheduler_get_next_job(current_policy);
        
        if (next_job == NULL) {
            pthread_cond_wait(&job_available, &queue_mutex);
            pthread_mutex_unlock(&queue_mutex);
            continue;
        }
        
        next_job->status = JOB_RUNNING;
        next_job->start_time = current_sim_time;
        pthread_mutex_unlock(&queue_mutex);
        
        dispatcher_assign_job(worker, next_job);
        
        pthread_mutex_lock(&queue_mutex);
        next_job->finish_time = next_job->start_time + next_job->estimated_runtime;
        next_job->status = JOB_DONE;
        completed_jobs++;
        
        if (completed_jobs >= total_jobs) {
            pthread_cond_broadcast(&job_available); 
        }
        pthread_mutex_unlock(&queue_mutex);
    }
    return NULL;
}

void* clock_loop(void* arg) {
    (void)arg;
    while (1) {
        usleep(TICK_US); 
        pthread_mutex_lock(&queue_mutex);
        if (completed_jobs >= total_jobs) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        current_sim_time++;
        pthread_cond_broadcast(&job_available); 
        pthread_mutex_unlock(&queue_mutex);
    }
    return NULL;
}

void load_jobs(const char* filename) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        exit(1);
    }
    
    char line[256];
    if (!fgets(line, sizeof(line), file)) {
        fprintf(stderr, "Error: CSV file is empty or missing header.\n");
        fclose(file);
        exit(1);
    }
    
    while (fgets(line, sizeof(line), file)) {
        if (total_jobs >= MAX_JOBS) {
            fprintf(stderr, "Warning: Job limit (%d) reached. Remaining jobs ignored.\n", MAX_JOBS);
            break;
        }
        job_t* job = &jobs[total_jobs];
        job->start_time = 0;
        job->finish_time = 0;
        job->status = JOB_WAITING;
        int parsed = sscanf(line, "%d,%31[^,],%d,%d,%d,%63[^\n]",
               &job->job_id, job->seller_id, &job->arrival_time,
               &job->estimated_runtime, &job->priority, job->job_type);
        if (parsed < 6) {
            fprintf(stderr, "Warning: Skipping malformed CSV line: %s", line);
            continue;
        }
        total_jobs++;
    }
    fclose(file);
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        printf("Usage: %s <csv_file> <policy(fifo/sjf/priority)> <workers>\n", argv[0]);
        return 1;
    }
    
    load_jobs(argv[1]);
    
    if (strcmp(argv[2], "fifo") == 0) current_policy = POLICY_FIFO;
    else if (strcmp(argv[2], "sjf") == 0) current_policy = POLICY_SJF;
    else if (strcmp(argv[2], "priority") == 0) current_policy = POLICY_PRIORITY;
    else {
        printf("Unknown policy. Use fifo, sjf, or priority.\n");
        return 1;
    }
    
    num_workers = atoi(argv[3]);
    if (num_workers <= 0) {
        fprintf(stderr, "Error: Number of workers must be a positive integer.\n");
        return 1;
    }
    worker_t* workers = malloc(num_workers * sizeof(worker_t));
    if (!workers) {
        fprintf(stderr, "Error: Failed to allocate memory for workers.\n");
        return 1;
    }
    
    pthread_t clock_thread;
    pthread_create(&clock_thread, NULL, clock_loop, NULL);
    
    for (int i = 0; i < num_workers; i++) {
        workers[i].worker_id = i;
        workers[i].busy_time = 0;
        pthread_create(&workers[i].thread, NULL, worker_loop, &workers[i]);
    }
    
    for (int i = 0; i < num_workers; i++) {
        pthread_join(workers[i].thread, NULL);
    }
    pthread_join(clock_thread, NULL);
    
    int total_waiting_time = 0;
    int total_turnaround_time = 0;
    int total_worker_busy_time = 0;
    long total_runtime = 0;
    
    for (int i = 0; i < total_jobs; i++) {
        int waiting_time = jobs[i].start_time - jobs[i].arrival_time; 
        int turnaround_time = jobs[i].finish_time - jobs[i].arrival_time; 
        
        total_waiting_time += waiting_time;
        total_turnaround_time += turnaround_time;
        total_runtime += jobs[i].estimated_runtime;
    }
    
    for (int i = 0; i < num_workers; i++) {
        total_worker_busy_time += workers[i].busy_time;
    }
    
    double avg_runtime = (double)total_runtime / total_jobs;
    int starvation_threshold = (int)(2 * avg_runtime); 
    int starvation_risk_jobs = 0;
    
    for (int i = 0; i < total_jobs; i++) {
        int waiting_time = jobs[i].start_time - jobs[i].arrival_time;
        if (waiting_time > starvation_threshold) { 
            starvation_risk_jobs++;
        }
    }
    
    double avg_waiting_time = (double)total_waiting_time / total_jobs;
    double avg_turnaround_time = (double)total_turnaround_time / total_jobs;
    double throughput = (double)total_jobs / current_sim_time; 
    double utilization = ((double)total_worker_busy_time / (num_workers * current_sim_time)) * 100.0; 
    
    printf("\n--- Run Summary ---\n");
    printf("Policy: %s (with Aging)\n", argv[2]);
    printf("Workers: %d\n", num_workers);
    printf("Total jobs: %d\n", total_jobs);
    printf("Total simulation time: %d\n", current_sim_time);
    printf("Average waiting time: %.2f\n", avg_waiting_time);
    printf("Average turnaround time: %.2f\n", avg_turnaround_time);
    printf("Throughput: %.3f jobs/unit time\n", throughput);
    printf("Worker utilization: %.2f%%\n", utilization);
    printf("Starvation-risk jobs: %d\n", starvation_risk_jobs);
    
    free(workers);
    return 0;
}