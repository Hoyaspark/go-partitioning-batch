package worker

import (
	"github.com/Hoyaspark/go-partitioning-batch/worker/parallel"
	"github.com/Hoyaspark/go-partitioning-batch/worker/step"
)

type Worker interface {
	Handle(pr parallel.Parallel, opt workerOption) (int64, int64, error)
	ParallelDB() step.ReaderDB
	GetReadQuery(string) string
}
