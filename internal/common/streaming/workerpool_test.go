package streaming

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

type testJob struct {
	key        string
	shouldFail bool
	executed   bool
	mu         sync.Mutex
}

func (t *testJob) Key() string {
	return t.key
}

func (t *testJob) Execute(ctx context.Context) error {
	t.mu.Lock()
	t.executed = true
	t.mu.Unlock()

	if t.shouldFail {
		return errors.New("test job fail")
	}

	// simulate some work
	time.Sleep(10 * time.Millisecond)
	return nil
}

func (t *testJob) Executed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.executed
}

func TestWorkerPool_BasicUsage(t *testing.T) {
	pool := newWorkerPool(2)
	ctx := context.Background()

	err := pool.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start workerpool: %v", err)
	}
	defer pool.Stop()

	jobs := []*testJob{
		{key: "key1", shouldFail: false},
		{key: "key2", shouldFail: false},
		{key: "key1", shouldFail: false},
		{key: "key2", shouldFail: false},
		{key: "key3", shouldFail: false},
	}

	for _, job := range jobs {
		err := pool.Submit(ctx, job)
		if err != nil {
			t.Fatalf("failed to submit job: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	for i, job := range jobs {
		if !job.Executed() {
			t.Errorf("job %d not executed", i)
		}
	}
}

func TestWorkerPool_ErrorHandling(t *testing.T) {
	pool := newWorkerPool(2)
	ctx := context.Background()

	err := pool.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start workerpool: %v", err)
	}
	defer pool.Stop()

	jobs := []*testJob{
		{key: "key1", shouldFail: false},
		{key: "key2", shouldFail: true}, // This job will fail
		{key: "key3", shouldFail: false},
	}

	for _, job := range jobs {
		err := pool.Submit(ctx, job)
		if err != nil {
			t.Fatalf("failed to submit job: %v", err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	// All jobs should be executed, even the failing one
	for i, job := range jobs {
		if !job.Executed() {
			t.Errorf("job %d not executed (even failing jobs should be executed)", i)
		}
	}
}

func TestWorkerPool_NotStarted(t *testing.T) {
	pool := newWorkerPool(2)
	ctx := context.Background()

	job := &testJob{key: "key1", shouldFail: false}

	// Try to submit without starting
	err := pool.Submit(ctx, job)
	if err == nil {
		t.Error("expected error when submitting to non-started pool")
	}

	// Try batch submit without starting
	err = pool.SubmitBatch(ctx, []Job{job})
	if err == nil {
		t.Error("expected error when batch submitting to non-started pool")
	}
}

func TestWorkerPool_Lifecycle(t *testing.T) {
	pool := newWorkerPool(2)
	ctx := context.Background()

	// Test double start
	err := pool.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start workerpool: %v", err)
	}

	err = pool.Start(ctx)
	if err == nil {
		t.Error("expected error when starting already started pool")
	}

	// Test stop multiple times (should be idempotent)
	err = pool.Stop()
	if err != nil {
		t.Errorf("failed to stop pool: %v", err)
	}

	err = pool.Stop()
	if err != nil {
		t.Errorf("second stop should be idempotent: %v", err)
	}
}

func TestWorkerPool_ContextCancellation(t *testing.T) {
	pool := newWorkerPool(2)
	ctx, cancel := context.WithCancel(context.Background())

	err := pool.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start workerpool: %v", err)
	}
	defer pool.Stop()

	// Cancel context immediately
	cancel()

	job := &testJob{key: "key1", shouldFail: false}

	// Try to submit after context cancellation
	err = pool.Submit(ctx, job)
	if err == nil {
		t.Error("expected error when submitting with cancelled context")
	}
}

func TestWorkerPool_HighConcurrency(t *testing.T) {
	pool := newWorkerPool(5)
	ctx := context.Background()

	err := pool.Start(ctx)
	if err != nil {
		t.Fatalf("failed to start workerpool: %v", err)
	}
	defer pool.Stop()

	// Create many jobs
	numJobs := 100
	jobs := make([]*testJob, numJobs)
	for i := 0; i < numJobs; i++ {
		jobs[i] = &testJob{
			key:        fmt.Sprintf("key%d", i%10), // 10 different keys
			shouldFail: false,
		}
	}

	// Submit all jobs concurrently
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(j *testJob) {
			defer wg.Done()
			err := pool.Submit(ctx, j)
			if err != nil {
				t.Errorf("failed to submit job: %v", err)
			}
		}(job)
	}

	wg.Wait()
	time.Sleep(200 * time.Millisecond) // Wait for processing

	// Check all jobs executed
	for i, job := range jobs {
		if !job.Executed() {
			t.Errorf("job %d not executed", i)
		}
	}
}
