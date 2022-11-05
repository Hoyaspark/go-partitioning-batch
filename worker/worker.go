package worker

import (
	"context"
	"github.com/Hoyaspark/go-partitioning-batch/pkg/er"
	"github.com/Hoyaspark/go-partitioning-batch/worker/monitoring"
	"github.com/Hoyaspark/go-partitioning-batch/worker/parallel"
	"github.com/Hoyaspark/go-partitioning-batch/worker/step"
	"github.com/Hoyaspark/go-partitioning-batch/worker/step/processor"
	"github.com/Hoyaspark/go-partitioning-batch/worker/step/reader"
	"github.com/Hoyaspark/go-partitioning-batch/worker/step/writer"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"runtime"
	"sync/atomic"
	"time"
)

var errEmptyStep = errors.New("empty step")

type WORKERS map[string]Worker

type worker[T step.DocProcessor[R, J], R, K any, J step.ProcessorParam[J]] struct {
	workerOpt        workerOption
	stepOpt          step.Option
	processorParam   step.ProcessorParam[J]
	readDB           step.ReaderDB
	writeDB          step.WriterStorage[K]
	parallelTypeFunc parallel.ParallelTypeFunc
}

// NewWorker is function that returns Worker created to run batch processes in parallel
// T is Type returned when reading(step.Reader)
// R is Type both processed by the process(step.Processor) before writing and returned and passed as a parameter when writing(step.Writer)
// K is Type that returns an object to be stored in storage when writing (example. sqlx.DB, elastic.Client) (step.WriterStorage)
// J is Type passed as a parameter when processing(step.ProcessorParam)
// If you don't need step.ProcessorParam, pass both step.EmptyDocProcessorParamType as generic type and step.EmptyDocProcessorParam as parameter
func NewWorker[T step.DocProcessor[R, J], R, K any, J step.ProcessorParam[J]](
	readDB step.ReaderDB, writeDB step.WriterStorage[K], processorParam step.ProcessorParam[J],
	parallelTypeFunc parallel.ParallelTypeFunc, stepOpt step.Option) Worker {

	return &worker[T, R, K, J]{
		stepOpt:          stepOpt,
		processorParam:   processorParam,
		readDB:           readDB,
		writeDB:          writeDB,
		parallelTypeFunc: parallelTypeFunc,
	}
}

func (m *worker[T, R, K, J]) ParallelDB() step.ReaderDB {
	return m.readDB
}

func (m *worker[T, R, K, J]) GetReadQuery(sourceName string) string {
	return m.readDB.GetReadQuery(sourceName)
}

func (m *worker[T, R, K, J]) Handle(pr parallel.Parallel, workerOpt workerOption) (int64, int64, error) {
	var totalRow, totalAffected int64

	op := er.GetOperator()

	m.workerOpt = workerOpt

	if !m.workerOpt.isSet {
		log.Error().Msg("need to set required settings [indexer,consumer]")
		return 0, 0, er.New("need to set required settings [indexer,consumer]", op, er.KindFatal)
	}

	parallelCtx, err := pr.Partition(m.parallelTypeFunc)
	if err != nil {
		return 0, 0, er.WrapOp(err, op)
	}

	wm := monitoring.NewMonitoring(len(parallelCtx))

	now := time.Now()

	log.Info().Int("GOMAXPROCS", runtime.GOMAXPROCS(runtime.NumCPU())).Msg("[worker monitoring] set GOMAXPROCS")

	for _, parCtx := range parallelCtx {
		go m.execute(context.Background(), parCtx, wm)
	}

	for {
		select {
		case err := <-wm.ReceiveErr():
			log.Error().Err(err).Msg("[worker monitoring] occurred error")
			return 0, 0, er.WrapOp(err, op)
		case r := <-wm.ReceiveResult():
			atomic.AddInt64(&totalAffected, r.RowAffectedCount())
			atomic.AddInt64(&totalRow, r.RowCount())
			log.Info().Msgf("[worker monitoring] received from [%s]. totalRow: %v, totalAffected: %v, elapsed time : %s", r.ContextName(), totalRow, totalAffected, time.Now().Sub(now))
			if wm.IsFinish() {
				return totalRow, totalAffected, nil
			}
		}
	}
}

func (m *worker[T, R, K, J]) step() (step.Step, error) {
	op := er.GetOperator()

	var newReader step.NewReader[T, R, J]

	switch m.stepOpt.ReaderType() {
	case step.PagingRead:
		newReader = reader.NewPagingDocReader[T, R, J](m.stepOpt.PageSize(), m.workerOpt.query, m.readDB)
	case step.FullRead:
		newReader = reader.NewFullDocReader[T, R, J](m.workerOpt.query, m.readDB)
	default:
		return nil, er.WrapOp(errors.New("need to set required settings [step.ReaderType]"), op)
	}

	switch m.workerOpt.workerType {
	case consumer:
		return step.NewStep[T, R, K, J](
			m.stepOpt.ChunkSize(),
			newReader.New(),
			m.processorParam,
			processor.NewProcessor[T, R, J](),
			writer.NewSqlxDocWriter[R, K]("insert query here", m.writeDB)), nil
	default:
		return nil, er.WrapOp(errEmptyStep, op)
	}
}

func (m *worker[T, R, K, J]) execute(ctx context.Context, partCtx parallel.Partition, wm monitoring.WorkerMonitoring) {

	clone, err := m.step()
	if err != nil {
		wm.SendErr(err)
		return
	}

	if err := clone.Proceed(ctx, partCtx, wm); err != nil {
		wm.SendErr(err)
	}
}
