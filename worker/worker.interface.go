package worker

import (
	"go-partitioning-batch/worker/parallel"
	"go-partitioning-batch/worker/step"
)

type Worker interface {
	Handle(pr parallel.Parallel, opt workerOption) (int64, int64, error)
	ParallelDB() step.ReaderDB
	GetReadQuery(string) string
}
