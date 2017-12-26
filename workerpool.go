package workerpool

import (
	"fmt"
	"sync"
)

type Job interface{}
type JobRet interface{}

type Worker interface {
	Work(job interface{}) interface{}
	start()
	stop()
	close()
	getChWorker() ChWorker
}

type SyncJob struct {
	job      Job
	chJobRet chan JobRet
}

type ChWorker struct {
	chAsyncJob chan Job
	chSyncJob  chan SyncJob
	chQuit     chan bool
}

type BaseWorker struct {
	ChWorker
}

func (w BaseWorker) getChWorker() ChWorker {
	return w.ChWorker
}

func (w *BaseWorker) start() {
	if w.chAsyncJob == nil {
		w.chAsyncJob = make(chan Job)
	}
	if w.chSyncJob == nil {
		w.chSyncJob = make(chan SyncJob)
	}
	if w.chQuit == nil {
		w.chQuit = make(chan bool)
	}
}

func (w BaseWorker) stop() {
	go func() {
		w.chQuit <- true
	}()
}

func (w *BaseWorker) close() {
	close(w.chAsyncJob)
	close(w.chSyncJob)
	close(w.chQuit)
}

type WorkerPool struct {
	chJob        chan Job
	chJobRet     chan JobRet
	chStop       chan bool
	chWorkerPool chan ChWorker
	wgJob        sync.WaitGroup
	maxWorkers   int
	maxJobs      int
	workers      []Worker
}

func NewWorkerPool(maxWorkers, maxJobs int) *WorkerPool {
	return &WorkerPool{
		chJob:        make(chan Job, maxJobs),
		chJobRet:     make(chan JobRet, maxJobs*2),
		chStop:       make(chan bool),
		chWorkerPool: make(chan ChWorker, maxWorkers),
		maxWorkers:   maxWorkers,
		maxJobs:      maxJobs,
		workers:      make([]Worker, 0, maxWorkers),
	}
}

func (wp *WorkerPool) startWorker(w Worker) {
	go func(w Worker) {
		w.start()
		chWorker := w.getChWorker()
		chAsyncJob := chWorker.chAsyncJob
		chSyncJob := chWorker.chSyncJob
		chQuit := chWorker.chQuit
		for {
			wp.chWorkerPool <- chWorker
			select {
			case asyncjob := <-chAsyncJob:
				wp.chJobRet <- w.Work(asyncjob)
				wp.wgJob.Done()
			case syncjob := <-chSyncJob:
				syncjob.chJobRet <- w.Work(syncjob.job)
			case <-chQuit:
				w.close()
				return
			}
		}
	}(w)
}

func (wp *WorkerPool) stopWorker(w Worker) {
	w.stop()
}

func (wp *WorkerPool) AddWorker(w Worker) error {
	if len(wp.workers) >= wp.maxWorkers {
		return fmt.Errorf("workpool is full.")
	}
	wp.workers = append(wp.workers, w)
	return nil
}

func (wp *WorkerPool) AddWorkers(w []Worker) error {
	if len(wp.workers)+len(w) > wp.maxWorkers {
		return fmt.Errorf("workpool is full.")
	}
	wp.workers = append(wp.workers, w...)
	return nil
}

func (wp *WorkerPool) AddJob(job Job) {
	wp.wgJob.Add(1)
	go func(job Job) {
		wp.chJob <- job
	}(job)
}

func (wp *WorkerPool) DoJob(job Job) JobRet {
	chWorker := <-wp.chWorkerPool
	chJobRet := make(chan JobRet)
	syncJob := SyncJob{job: job, chJobRet: chJobRet}
	go func(syncJob SyncJob, chW ChWorker) {
		chW.chSyncJob <- syncJob
	}(syncJob, chWorker)
	return <-chJobRet
}

func (wp *WorkerPool) Start() {
	for _, w := range wp.workers {
		wp.startWorker(w)
	}
	go wp.dispatch()
}

func (wp WorkerPool) Stop() {
	wp.chStop <- true
}

func (wp WorkerPool) close() {
	close(wp.chJob)
	close(wp.chJobRet)
	close(wp.chWorkerPool)
	close(wp.chStop)
}

func (wp WorkerPool) GetJobRet() JobRet {
	select {
	case jobRet := <-wp.chJobRet:
		return jobRet
	default:
		return nil
	}
}

func (wp *WorkerPool) dispatch() {
	for {
		select {
		case job := <-wp.chJob:
			go func(job Job) {
				chWorker := <-wp.chWorkerPool
				chWorker.chAsyncJob <- job
			}(job)
		case <-wp.chStop:
			wp.close()
			return
		}
	}
}

func (wp *WorkerPool) Wait() {
	wp.wgJob.Wait()
}
