package streaming

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
)

type Job interface {
	Execute(ctx context.Context) error
	Key() string
}

type WorkerPool interface {
	Start(ctx context.Context) error
	Stop() error
	Submit(ctx context.Context, job Job) error
	SubmitBatch(ctx context.Context, jobs []Job) error
}

type worker struct {
	id       int
	jobQueue chan Job
	quit     chan struct{}
	wg       *sync.WaitGroup
}

func (w *worker) run(ctx context.Context) {
	defer w.wg.Done()

	for {
		select {
		case job, ok := <-w.jobQueue:
			if !ok {
				return
			}

			if err := job.Execute(ctx); err != nil {
				fmt.Printf("worker %d execute job err:%v\n", w.id, err)
				continue
			}
		case <-w.quit:
			return
		case <-ctx.Done():
			return
		}
	}
}

func newWorkerPool(maxWorkers int) *workerPool {
	if maxWorkers <= 0 {
		maxWorkers = 1
	}

	return &workerPool{
		maxWorkers: maxWorkers,
		jobQueue:   make(chan Job, maxWorkers),
		quit:       make(chan struct{}),
	}
}

type workerPool struct {
	workers    []*worker
	jobQueue   chan Job
	quit       chan struct{}
	wg         sync.WaitGroup
	maxWorkers int
	started    bool
	mu         sync.RWMutex
}

func (p *workerPool) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.started {
		return fmt.Errorf("worker pool already started")
	}

	p.workers = make([]*worker, p.maxWorkers)

	for i := 0; i < p.maxWorkers; i++ {
		w := &worker{
			id:       i,
			jobQueue: p.jobQueue,
			quit:     make(chan struct{}),
			wg:       &p.wg,
		}

		p.workers[i] = w
		p.wg.Add(1)
		go w.run(ctx)
	}

	p.started = true
	return nil
}

func (p *workerPool) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.started {
		return nil
	}

	close(p.jobQueue)

	for _, w := range p.workers {
		close(w.quit)
	}

	p.wg.Wait()

	p.workers = nil
	p.jobQueue = nil
	p.quit = nil
	p.started = false

	return nil
}

func (p *workerPool) Submit(ctx context.Context, job Job) error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if !p.started {
		return fmt.Errorf("worker pool not started")
	}

	select {
	case p.jobQueue <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *workerPool) SubmitBatch(ctx context.Context, jobs []Job) error {
	for _, job := range jobs {
		if err := p.Submit(ctx, job); err != nil {
			return err
		}
	}

	return nil
}

func newHashBasedWorkerPool(maxWorkers int) *hashBasedWorkerPool {
	if maxWorkers <= 0 {
		maxWorkers = 1
	}

	pool := &hashBasedWorkerPool{
		workerQueues: make([]chan Job, maxWorkers),
		workers:      make([]*hashBasedWorker, 0, maxWorkers),
		maxWorkers:   maxWorkers,
	}

	for i := 0; i < maxWorkers; i++ {
		pool.workerQueues[i] = make(chan Job, 100)
	}

	return pool
}

// Simple hash-based worker pool that ensures jobs with same key go to same worker
type hashBasedWorkerPool struct {
	workerQueues []chan Job
	workers      []*hashBasedWorker
	maxWorkers   int
	started      bool
	mutex        sync.RWMutex
	wg           sync.WaitGroup
}

// Simple hash function to determine which worker should handle a key
func (p *hashBasedWorkerPool) getWorkerForKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % p.maxWorkers
}

func (p *hashBasedWorkerPool) Start(ctx context.Context) error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.started {
		return fmt.Errorf("hash-based worker pool already started")
	}

	// Start workers
	for i := 0; i < p.maxWorkers; i++ {
		w := &hashBasedWorker{
			id:       i,
			jobQueue: p.workerQueues[i],
			quit:     make(chan struct{}),
			wg:       &p.wg,
		}

		p.workers = append(p.workers, w)
		p.wg.Add(1)
		go w.run(ctx)
	}

	p.started = true
	return nil
}

func (p *hashBasedWorkerPool) Stop() error {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if !p.started {
		return nil
	}

	// Close all worker queues
	for i := 0; i < p.maxWorkers; i++ {
		close(p.workerQueues[i])
	}

	// Stop all workers
	for _, w := range p.workers {
		close(w.quit)
	}

	p.wg.Wait()

	p.workers = nil
	p.started = false

	return nil
}

func (p *hashBasedWorkerPool) Submit(ctx context.Context, job Job) error {
	p.mutex.RLock()
	defer p.mutex.RUnlock()

	if !p.started {
		return fmt.Errorf("hash-based worker pool not started")
	}

	// Get the worker assigned to this key
	workerID := p.getWorkerForKey(job.Key())

	// Send job to the assigned worker
	select {
	case p.workerQueues[workerID] <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *hashBasedWorkerPool) SubmitBatch(ctx context.Context, jobs []Job) error {
	for _, job := range jobs {
		if err := p.Submit(ctx, job); err != nil {
			return err
		}
	}
	return nil
}

type hashBasedWorker struct {
	id       int
	jobQueue chan Job
	quit     chan struct{}
	wg       *sync.WaitGroup
}

func (w *hashBasedWorker) run(ctx context.Context) {
	defer w.wg.Done()

	for {
		select {
		case job, ok := <-w.jobQueue:
			if !ok {
				return
			}

			if err := job.Execute(ctx); err != nil {
				fmt.Printf("hash-based worker %d execute job err:%v\n", w.id, err)
			}
		case <-w.quit:
			return
		case <-ctx.Done():
			return
		}
	}
}
