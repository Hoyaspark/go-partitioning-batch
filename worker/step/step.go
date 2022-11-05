package step

import (
	"context"
	"go-partitioning-batch/pkg/er"
	"go-partitioning-batch/worker/monitoring"
	"go-partitioning-batch/worker/parallel"
	"sync/atomic"
)

const LogIntervalSize = 30000

type EmptyDocProcessorParamType string

func (e EmptyDocProcessorParamType) GetProcessorParam() (*EmptyDocProcessorParamType, error) {
	return &e, nil
}

var EmptyDocProcessorParam EmptyDocProcessorParamType

type step[T DocProcessor[R, J], R, K any, J ProcessorParam[J]] struct {
	chunkSize      int64
	reader         Reader[T, R, J]
	processorParam ProcessorParam[J]
	processor      Processor[T, R, J]
	writer         Writer[R, K]
}

// NewStep is function that returns Step created to run the batch process
// T is Type returned when reading(Reader)
// R is Type both processed by the process(Processor) before writing and returned and passed as a parameter when writing(Writer)
// K is Type that returns an object to be stored in storage when writing (example. sqlx.DB, elastic.Client) (WriterStorage)
// J is Type passed as a parameter when processing(ProcessorParam)
// If you don't need step.ProcessorParam, pass both step.EmptyDocProcessorParamType as generic type and step.EmptyDocProcessorParam as parameter
func NewStep[T DocProcessor[R, J], R, K any, J ProcessorParam[J]](
	chunkSize int64,
	reader Reader[T, R, J],
	processorParam ProcessorParam[J],
	processor Processor[T, R, J],
	writer Writer[R, K]) Step {
	return &step[T, R, K, J]{
		chunkSize:      chunkSize,
		reader:         reader,
		processorParam: processorParam,
		processor:      processor,
		writer:         writer,
	}
}

func (s step[T, R, K, J]) Proceed(ctx context.Context, pCtx parallel.Partition, wm monitoring.WorkerMonitoring) error {
	var rowAffectedCount, rowCount int64
	defer wm.Finish()

	op := er.GetOperator()
	buf := make([]R, 0, s.chunkSize)

	for {
		item, done, err := s.reader.Read(ctx, pCtx)
		if err != nil {
			return er.WrapOp(err, op)
		}

		if done {
			break
		}

		if item != nil {
			param, err := s.processorParam.GetProcessorParam()
			if err != nil {
				return er.WrapOp(err, op)
			}

			refineItem, err := s.processor.Process(*item, param, pCtx)
			if err != nil {
				return er.WrapOp(err, op)
			}

			buf = append(buf, *refineItem)
		}

		if int64(len(buf)) >= s.chunkSize {
			copyBuf := make([]R, len(buf))
			copy(copyBuf, buf)
			buf = make([]R, 0, s.chunkSize)

			rowsAff, err := s.writer.Write(copyBuf, pCtx)
			if err != nil {
				return er.WrapOp(err, op)
			}

			atomic.AddInt64(&rowCount, s.chunkSize)
			atomic.AddInt64(&rowAffectedCount, rowsAff)

			if rowCount/LogIntervalSize > 0 && rowCount%LogIntervalSize == 0 {
				wm.Send(monitoring.NewRowCountLog(pCtx.PartitionName(), rowAffectedCount, rowCount))
				rowCount, rowAffectedCount = 0, 0
			}
		}
	}

	if len(buf) > 0 {
		rowsAff, err := s.writer.Write(buf, pCtx)
		if err != nil {
			return er.WrapOp(err, op)
		}

		atomic.AddInt64(&rowCount, int64(len(buf)))
		atomic.AddInt64(&rowAffectedCount, rowsAff)
	}

	wm.Send(monitoring.NewRowCountLog(pCtx.PartitionName(), rowAffectedCount, rowCount))

	return nil
}
