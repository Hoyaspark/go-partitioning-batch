package parallel

import (
	"github.com/Hoyaspark/go-partitioning-batch/pkg/er"
	"github.com/rs/zerolog/log"
	"strconv"
)

type parallel struct {
	sourceName   string
	parallelSize int64
	db           ParallelDB
}

func NewParallel(sourceName string, db ParallelDB, parallelSize int64) Parallel {
	return &parallel{
		sourceName:   sourceName,
		parallelSize: parallelSize,
		db:           db,
	}
}

type ParallelTypeFunc func(*parallel) ([]Partition, error)

var (
	AutoIncrementId ParallelTypeFunc = partitionAutoIncrementId
	SortableId      ParallelTypeFunc = partitionSortableId
	None            ParallelTypeFunc = partitionNone
)

func partitionNone(p *parallel) ([]Partition, error) {
	parallelId := "pCtx[0]"

	log.Info().Msgf("[%s] works only one partition", parallelId)
	return []Partition{
		&partition{
			parallelId: parallelId,
		},
	}, nil
}

func partitionAutoIncrementId(p *parallel) ([]Partition, error) {
	op := er.GetOperator()

	pCtx, err := p.db.GetSortBy(p.sourceName)
	if err != nil {
		log.Err(err).Msg("failed to get sort by")
		return nil, er.WrapOp(err, op)
	}

	var pcs []Partition

	minId := pCtx.Min()
	maxId := pCtx.Max()

	targetSize := (maxId-minId)/p.parallelSize + 1

	number := 0
	start := minId
	end := start + targetSize - 1

	for start <= maxId {

		if end >= maxId {
			end = maxId
		}

		pcs = append(pcs, &partition{
			parallelId:    "pCtx" + strconv.Itoa(number),
			min:           start,
			max:           end,
			partitionType: AutoIncrementIdType,
		})

		log.Info().Msgf("[%s] divide into [min:%d] [max:%d]", "pCtx"+strconv.Itoa(number), start, end)
		start += targetSize
		end += targetSize
		number++
	}

	return pcs, nil
}

func partitionSortableId(p *parallel) ([]Partition, error) {

	op := er.GetOperator()

	pCtx, err := p.db.GetSortBy(p.sourceName)
	if err != nil {
		log.Err(err).Msg("failed to get sort by")
		return nil, er.WrapOp(err, op)
	}

	var pcs []Partition

	cnt := pCtx.Count()

	targetSize := cnt/p.parallelSize + 1

	number := 0
	start := int64(0)
	end := start + targetSize

	for start <= cnt {

		if end >= cnt {
			end = cnt
		}

		pcs = append(pcs, &partition{
			parallelId:    "pCtx" + strconv.Itoa(number),
			min:           targetSize, // limit
			max:           start,      // offset
			partitionType: SortableIdType,
		})

		log.Info().Msgf("[%s] divide into [limit:%d] [offset:%d]", "pCtx"+strconv.Itoa(number), targetSize, start)
		start += targetSize
		end += targetSize
		number++
	}

	return pcs, nil

}

func (p *parallel) Partition(ptf ParallelTypeFunc) ([]Partition, error) {
	return ptf(p)
}
