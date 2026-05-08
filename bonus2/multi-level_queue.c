#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <stdbool.h>

#define MAX_JOBS 1000
#define TICK_US 100000 // 0.1 seconds per simulated time unit

// --- Suggested Data Structures [cite: 95-119] ---
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
    int busy_time; // To track individual worker utilization
} worker_t;

typedef enum {
    POLICY_FIFO,
    POLICY_SJF,
    POLICY_PRIORITY,
    POLICY_MLQ // Multi-Level Queue Policy
} policy_t;

// --- Global Variables ---
job_t jobs[MAX_JOBS];
int total_jobs = 0;
int completed_jobs = 0;
int current_sim_time = 0;
policy_t current_policy;
int num_workers = 0;

// Synchronization [cite: 122, 123]
pthread_mutex_t queue_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t job_available = PTHREAD_COND_INITIALIZER;

// --- Function Prototypes ---
job_t* scheduler_get_next_job(policy_t policy);
void dispatcher_assign_job(worker_t* worker, job_t* job);
void* worker_loop(void* arg);
void* clock_loop(void* arg);

// --- Scheduler Implementation [cite: 86-90] ---
job_t* scheduler_get_next_job(policy_t policy) {
    job_t* best_job = NULL;

    if (policy == POLICY_MLQ) {
        // --- Multi-Level Queue with different policies per queue ---
        // Queue 1 (Highest): priority <= 2, scheduled by Priority + FCFS
        // Queue 2 (Medium):  priority == 3, scheduled by SJF + FCFS
        // Queue 3 (Lowest):  priority >= 4, scheduled by FCFS
        
        job_t* q1_best_job = NULL; // Best job from Queue 1 (Priority)
        job_t* q2_best_job = NULL; // Best job from Queue 2 (SJF)
        job_t* q3_best_job = NULL; // Best job from Queue 3 (FCFS)

        // Single pass to find the best candidate for each queue
        for (int i = 0; i < total_jobs; i++) {
            if (jobs[i].status == JOB_WAITING && jobs[i].arrival_time <= current_sim_time) {
                
                // --- Queue 1: Real-time/Interactive (Priority <= 2) ---
                if (jobs[i].priority <= 2) {
                    if (q1_best_job == NULL) {
                        q1_best_job = &jobs[i];
                    } else {
                        // Priority scheduling (lower number is higher priority)
                        if (jobs[i].priority < q1_best_job->priority) {
                            q1_best_job = &jobs[i];
                        } 
                        // FCFS as a tie-breaker for same priority
                        else if (jobs[i].priority == q1_best_job->priority && jobs[i].arrival_time < q1_best_job->arrival_time) {
                            q1_best_job = &jobs[i];
                        }
                    }
                } 
                // --- Queue 2: Batch/Normal (Priority == 3) ---
                else if (jobs[i].priority == 3) {
                    if (q2_best_job == NULL) {
                        q2_best_job = &jobs[i];
                    } else {
                        // Shortest Job First (SJF) scheduling
                        if (jobs[i].estimated_runtime < q2_best_job->estimated_runtime) {
                            q2_best_job = &jobs[i];
                        }
                        // FCFS as a tie-breaker for same runtime
                        else if (jobs[i].estimated_runtime == q2_best_job->estimated_runtime && jobs[i].arrival_time < q2_best_job->arrival_time) {
                            q2_best_job = &jobs[i];
                        }
                    }
                } 
                // --- Queue 3: Background/Low Priority (Priority >= 4) ---
                else { // priority >= 4
                    if (q3_best_job == NULL) {
                        q3_best_job = &jobs[i];
                    } else {
                        // First-Come, First-Served (FCFS) scheduling
                        if (jobs[i].arrival_time < q3_best_job->arrival_time) {
                            q3_best_job = &jobs[i];
                        }
                    }
                }
            }
        }

        // Return job from the highest-priority non-empty queue
        if (q1_best_job != NULL) return q1_best_job;
        if (q2_best_job != NULL) return q2_best_job;
        if (q3_best_job != NULL) return q3_best_job;
        
        return NULL; // No jobs available in any queue

    } else {
        // Original logic for FIFO, SJF, Priority
        for (int i = 0; i < total_jobs; i++) {
            // Only consider waiting jobs that have arrived
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
                        // Smaller number means higher priority [cite: 64]
                        if (jobs[i].priority < best_job->priority) {
                            best_job = &jobs[i];
                        }
                    }
                }
            }
        }
        return best_job;
    }
}

// --- Dispatcher Implementation [cite: 87, 88, 91] ---
void dispatcher_assign_job(worker_t* worker, job_t* job) {
    // Log start [cite: 79, 80]
    printf("[time=%d] Worker %d starts Job %d seller=%s runtime=%d priority=%d\n", 
           current_sim_time, worker->worker_id, job->job_id, job->seller_id, 
           job->estimated_runtime, job->priority);
           
    // Scaled simulation: sleep for runtime * 100000 microseconds 
    usleep(job->estimated_runtime * TICK_US);
    
    // Log finish [cite: 79, 82]
    printf("[time=%d] Worker %d finishes Job %d\n", 
           current_sim_time, worker->worker_id, job->job_id);
           
    worker->busy_time += job->estimated_runtime;
}

// --- Worker Loop [cite: 42, 92] ---
void* worker_loop(void* arg) {
    worker_t* worker = (worker_t*)arg;
    
    while (1) {
        pthread_mutex_lock(&queue_mutex);
        
        // Exit condition: all jobs are done
        if (completed_jobs >= total_jobs) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        
        job_t* next_job = scheduler_get_next_job(current_policy);
        
        if (next_job == NULL) {
            // Sleep when no job is available [cite: 126]
            pthread_cond_wait(&job_available, &queue_mutex);
            pthread_mutex_unlock(&queue_mutex);
            continue;
        }
        
        // Mark job as running and set start time
        next_job->status = JOB_RUNNING;
        next_job->start_time = current_sim_time;
        pthread_mutex_unlock(&queue_mutex);
        
        // Assign to dispatcher (outside of mutex to allow concurrent execution)
        dispatcher_assign_job(worker, next_job);
        
        // Update finish time and status
        pthread_mutex_lock(&queue_mutex);
        // finish_time is deterministic: start + runtime (avoids race with clock thread)
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

// --- Clock Thread for Simulating Time ---
void* clock_loop(void* arg) {
    (void)arg;
    while (1) {
        usleep(TICK_US); // 1 tick = 0.1s
        pthread_mutex_lock(&queue_mutex);
        if (completed_jobs >= total_jobs) {
            pthread_mutex_unlock(&queue_mutex);
            break;
        }
        current_sim_time++;
        // Wake up workers as new jobs might have arrived [cite: 126]
        pthread_cond_broadcast(&job_available); 
        pthread_mutex_unlock(&queue_mutex);
    }
    return NULL;
}

// --- Helper function to parse CSV [cite: 50-65] ---
void load_jobs(const char* filename) {
    FILE* file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        exit(1);
    }
    
    char line[256];
    // Skip header
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
        // Initialize fields to safe defaults before parsing
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

// --- Main Program ---
int main(int argc, char* argv[]) {
    if (argc != 4) {
        printf("Usage: %s <csv_file> <policy(fifo/sjf/priority/mlq)> <workers>\n", argv[0]);
        return 1;
    }
    
    // Parse arguments [cite: 76-78]
    load_jobs(argv[1]);
    
    if (strcmp(argv[2], "fifo") == 0) current_policy = POLICY_FIFO;
    else if (strcmp(argv[2], "sjf") == 0) current_policy = POLICY_SJF;
    else if (strcmp(argv[2], "priority") == 0) current_policy = POLICY_PRIORITY;
    else if (strcmp(argv[2], "mlq") == 0) current_policy = POLICY_MLQ;
    else {
        printf("Unknown policy. Use fifo, sjf, priority, or mlq.\n");
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
    
    // Start clock thread
    pthread_t clock_thread;
    pthread_create(&clock_thread, NULL, clock_loop, NULL);
    
    // Start worker pool [cite: 41, 75]
    for (int i = 0; i < num_workers; i++) {
        workers[i].worker_id = i;
        workers[i].busy_time = 0;
        pthread_create(&workers[i].thread, NULL, worker_loop, &workers[i]);
    }
    
    // Join threads
    for (int i = 0; i < num_workers; i++) {
        pthread_join(workers[i].thread, NULL);
    }
    pthread_join(clock_thread, NULL);
    
    // --- Metrics Calculation  ---
    int total_waiting_time = 0;
    int total_turnaround_time = 0;
    int total_worker_busy_time = 0;
    long total_runtime = 0;
    
    for (int i = 0; i < total_jobs; i++) {
        int waiting_time = jobs[i].start_time - jobs[i].arrival_time; // [cite: 136, 137]
        int turnaround_time = jobs[i].finish_time - jobs[i].arrival_time; // [cite: 139]
        
        total_waiting_time += waiting_time;
        total_turnaround_time += turnaround_time;
        total_runtime += jobs[i].estimated_runtime;
    }
    
    for (int i = 0; i < num_workers; i++) {
        total_worker_busy_time += workers[i].busy_time;
    }
    
    double avg_runtime = (double)total_runtime / total_jobs;
    int starvation_threshold = (int)(2 * avg_runtime); // [cite: 148]
    int starvation_risk_jobs = 0;
    
    for (int i = 0; i < total_jobs; i++) {
        int waiting_time = jobs[i].start_time - jobs[i].arrival_time;
        if (waiting_time > starvation_threshold) { // [cite: 146]
            starvation_risk_jobs++;
        }
    }
    
    double avg_waiting_time = (double)total_waiting_time / (double)total_jobs;
    double avg_turnaround_time = (double)total_turnaround_time / (double)total_jobs;
    double throughput = (double)total_jobs / (double)current_sim_time; // [cite: 142]
    double utilization = ((double)total_worker_busy_time / ((double)num_workers * (double)current_sim_time)) * 100.0; // [cite: 144]
    
    // --- Expected Output Summary [cite: 151-160] ---
    printf("\n--- Run Summary ---\n");
    printf("Policy: %s\n", argv[2]);
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
