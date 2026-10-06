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

type JobType string

const (
	Email JobType = "email"
)

type Job struct {
	ID      int     `json:"id"`
	Type    JobType `json:"type"`
	Payload string  `json:"payload"`
	Status  string  `json:"status"`
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
	case <-time.After(5 * time.Second):
		fmt.Println("work completed")
	case <-ctx.Done():
		fmt.Println("work cancelled")
	}

	return nil
}

func worker(workerId int, queue <-chan int) {
	for id := range queue {
		fmt.Printf("Worker of ID %v, executing job %v\n", workerId, id)
		processJob(id)
	}
}

func processJob(id int) {
	time.Sleep(2 * time.Second)

	mu.Lock()
	defer mu.Unlock()

	job, ok := jobs[id]
	if !ok {
		return
	}

	job.Status = "success"
	jobs[id] = job
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
	job.Status = "pending"
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
