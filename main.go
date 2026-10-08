package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

type JobStatus string

const (
	JobQueued    JobStatus = "queued"
	JobRunning   JobStatus = "running"
	JobSucceeded JobStatus = "succeeded"
	JobFailed    JobStatus = "failed"
)

type JobType string

const (
	Email JobType = "email"
)

type Job struct {
	ID         int        `json:"id"`
	Type       JobType    `json:"type"`
	Payload    string     `json:"payload"`
	Status     JobStatus  `json:"status"`
	CreatedAt  time.Time  `json:"createdAt"`
	StartedAt  *time.Time `json:"startedAt"`
	FinishedAt *time.Time `json:"finishedAt"`
}

type Jobs []Job

var (
	jobs     = make(map[int]Job, 0)
	jobQueue = make(chan int, 10)
	nextId   = 1
	mu       sync.Mutex
)

func slowWork(ctx context.Context) error {
	select {
	case <-time.After(2 * time.Second):
		fmt.Println("work completed")
		return nil
	case <-ctx.Done():
		fmt.Println("work cancelled")
		return ctx.Err()
	}
}

func worker(workerId int, queue <-chan int) {
	for id := range queue {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			1*time.Second)

		fmt.Printf("Worker of ID %v, executing job %v\n", workerId, id)

		err := processJob(ctx, id)
		cancel()

		fmt.Println(err)
	}
}

func processJob(ctx context.Context, id int) error {
	mu.Lock()

	job, ok := jobs[id]
	if !ok {
		mu.Unlock()
		return fmt.Errorf("job with an ID: %v, doesn't exist", id)
	}
	startedAt := time.Now()
	job.StartedAt = &startedAt
	job.Status = JobRunning
	jobs[id] = job

	mu.Unlock()

	err := slowWork(ctx)

	mu.Lock()

	if err != nil {
		job.Status = JobFailed
	} else {
		job.Status = JobSucceeded
	}
	finishedAt := time.Now()
	job.FinishedAt = &finishedAt
	jobs[id] = job

	mu.Unlock()

	return err
}

func checkHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	msg := map[string]string{"status": "ok"}
	if err := json.NewEncoder(w).Encode(msg); err != nil {
		log.Println("failed to encode health response:", err)
	}
}

func postJob(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	var job Job
	if jerr := json.NewDecoder(r.Body).Decode(&job); jerr != nil {
		http.Error(w, "Error decoding body", http.StatusBadRequest)
		return
	}
	mu.Lock()

	job.ID = nextId
	nextId++
	job.Status = JobQueued
	job.CreatedAt = time.Now()
	jobs[job.ID] = job

	select {
	case jobQueue <- job.ID:
		mu.Unlock()

		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(job); err != nil {
			log.Println("failed to submit a job:", err)
		}
	default:
		delete(jobs, job.ID)
		mu.Unlock()

		http.Error(w, "job queue full", http.StatusServiceUnavailable)
	}
}

func getJobById(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	param := r.PathValue("id")

	id, atoierr := strconv.Atoi(param)
	if atoierr != nil {
		http.Error(w, "wrong job id", http.StatusBadRequest)
		return
	}

	mu.Lock()
	job, ok := jobs[id]
	mu.Unlock()

	if !ok {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	if err := json.NewEncoder(w).Encode(job); err != nil {
		log.Println("failed to find a job:", err)
	}
}

func main() {
	http.HandleFunc("/health", checkHealth)
	http.HandleFunc("POST /jobs", postJob)
	http.HandleFunc("GET /jobs/{id}", getJobById)

	for wId := range 3 {
		go worker(wId, jobQueue)
	}

	log.Fatal(http.ListenAndServe(":8080", nil))
}
