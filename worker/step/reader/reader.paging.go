package reader

import (
	"context"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
	"go-partitioning-batch/pkg/er"
	"go-partitioning-batch/worker/parallel"
	"go-partitioning-batch/worker/step"
)

type pagingDocReader[T step.DocProcessor[R, J], R any, J step.ProcessorParam[J]] struct {
	pageSize    int64
	queryString string // need to set format "SELECT * FROM faqs WHERE faqs.id >= %d and fqs.id < %d"
	docReaderDB step.ReaderDB
	rows        *sqlx.Rows
	page        int64
	hasNext     bool
	readStatus  status
}

func NewPagingDocReader[T step.DocProcessor[R, J], R any, J step.ProcessorParam[J]](pageSize int64, queryString string, db step.ReaderDB) step.Reader[T, R, J] {
	return &pagingDocReader[T, R, J]{
		pageSize:    pageSize,
		queryString: queryString,
		docReaderDB: db,
		page:        0,
		hasNext:     false,
		readStatus:  statusReady,
	}
}

func (pdr *pagingDocReader[T, R, J]) New() step.Reader[T, R, J] {
	return &pagingDocReader[T, R, J]{
		pageSize:    pdr.pageSize,
		queryString: pdr.queryString,
		docReaderDB: pdr.docReaderDB,
		page:        pdr.page,
		hasNext:     pdr.hasNext,
		readStatus:  pdr.readStatus,
	}
}

func (pdr *pagingDocReader[T, R, J]) Read(ctx context.Context, partCtx parallel.Partition) (*T, bool, error) {
	op := er.GetOperator()

	if !pdr.hasNext && pdr.readStatus == statusReady {

		from, to := pdr.getPagination(partCtx)

		from, to = pdr.checkLimitPage(from, to, partCtx)

		if err := pdr.read(ctx, from, to); err != nil {
			return nil, false, er.WrapOp(err, op)
		}

		item, err := pdr.getItem()
		return item, false, err
	}

	if !pdr.hasNext && pdr.readStatus == statusFinish {

		pdr.rows.Close()

		return nil, true, nil
	}

	item, err := pdr.getItem()

	return item, false, err
}

func (pdr *pagingDocReader[T, R, J]) getItem() (*T, error) {
	op := er.GetOperator()

	hasNext := pdr.rows.Next()
	pdr.hasNext = hasNext

	if !hasNext {
		return nil, nil
	}

	var item T
	if err := pdr.rows.StructScan(&item); err != nil {
		log.Err(err).Msgf("scan error")
		return nil, er.WrapOp(err, op)
	}

	return &item, nil

}

func (pdr *pagingDocReader[T, R, J]) getPagination(partCtx parallel.Partition) (int64, int64) {
	switch partCtx.Type() {
	case parallel.AutoIncrementIdType:
		from := partCtx.Min() + (pdr.page * pdr.pageSize)
		to := from + pdr.pageSize - 1

		return from, to
	case parallel.SortableIdType:
		from := pdr.pageSize                            // limit
		to := partCtx.Max() + (pdr.page * pdr.pageSize) // offset

		return from, to
	}

	return 0, 0
}

func (pdr *pagingDocReader[T, R, J]) read(ctx context.Context, from, to any) error {
	op := er.GetOperator()

	if pdr.rows != nil {
		pdr.rows.Close()
	}

	q := fmt.Sprintf(pdr.queryString, from, to)

	rows, err := pdr.docReaderDB.ReadDB().QueryxContext(ctx, q)
	if err != nil {
		log.Err(err).Msgf("query: %s", q)
		return er.WrapOp(err, op)
	}

	pdr.rows = rows

	pdr.page++

	return nil
}

func (pdr *pagingDocReader[T, R, J]) checkLimitPage(from, to int64, parCtx parallel.Partition) (int64, int64) {

	switch parCtx.Type() {
	case parallel.AutoIncrementIdType:
		if to >= parCtx.Max() {
			pdr.readStatus = statusFinish
			return from, parCtx.Max()
		}

		return from, to
	case parallel.SortableIdType:
		if from+to >= parCtx.Max()+parCtx.Min() {
			pdr.readStatus = statusFinish
			return parCtx.Max() + parCtx.Min() - to, to
		}
	}

	return from, to
}
