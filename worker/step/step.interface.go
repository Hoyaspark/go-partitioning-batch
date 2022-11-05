package step

import (
	"context"
	"github.com/jmoiron/sqlx"
	"go-partitioning-batch/worker/monitoring"
	"go-partitioning-batch/worker/parallel"
)

type WriterStorage[K any] interface {
	GetClient() *K
}

type ReaderDB interface {
	parallel.ParallelDB
	GetReadQuery(string) string // Must returns Where range <= and >=
	ReadDB() *sqlx.DB
}

type Step interface {
	Proceed(context.Context, parallel.Partition, monitoring.WorkerMonitoring) error
}

type Option interface {
	PageSize() int64
	ChunkSize() int64
	ReaderType() readerType
}

type NewReader[T DocProcessor[R, J], R any, J ProcessorParam[J]] interface {
	New() Reader[T, R, J]
}

type Reader[T DocProcessor[R, J], R any, J ProcessorParam[J]] interface {
	NewReader[T, R, J]
	Read(context.Context, parallel.Partition) (*T, bool, error)
}

type ProcessorParam[J any] interface {
	GetProcessorParam() (*J, error)
}

type Processor[T DocProcessor[R, J], R any, J ProcessorParam[J]] interface {
	Process(T, *J, parallel.Partition) (*R, error)
}

type DocProcessor[R any, J ProcessorParam[J]] interface {
	ToModel(*J) (*R, error)
}

type Writer[R, K any] interface {
	Write([]R, parallel.Partition) (int64, error)
}
