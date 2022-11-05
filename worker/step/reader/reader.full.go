package reader

import (
	"context"
	"github.com/Hoyaspark/go-partitioning-batch/pkg/er"
	"github.com/Hoyaspark/go-partitioning-batch/worker/parallel"
	"github.com/Hoyaspark/go-partitioning-batch/worker/step"
	"github.com/jmoiron/sqlx"
	"github.com/rs/zerolog/log"
)

type fullDocReader[T step.DocProcessor[R, J], R any, J step.ProcessorParam[J]] struct {
	queryString string // need to set format "SELECT * FROM faqs WHERE faqs.id >= %d and fqs.id < %d"
	docReaderDB step.ReaderDB
	rows        *sqlx.Rows
	readStatus  status
}

func NewFullDocReader[T step.DocProcessor[R, J], R any, J step.ProcessorParam[J]](queryString string, db step.ReaderDB) step.Reader[T, R, J] {
	return &fullDocReader[T, R, J]{
		queryString: queryString,
		docReaderDB: db,
		readStatus:  statusReady,
	}
}

func (fdr *fullDocReader[T, R, J]) New() step.Reader[T, R, J] {
	return &fullDocReader[T, R, J]{
		queryString: fdr.queryString,
		docReaderDB: fdr.docReaderDB,
		readStatus:  fdr.readStatus,
	}
}

func (fdr *fullDocReader[T, R, J]) Read(ctx context.Context, partCtx parallel.Partition) (*T, bool, error) {
	op := er.GetOperator()

	if fdr.readStatus == statusReady {

		if err := fdr.read(ctx); err != nil {
			return nil, false, er.WrapOp(err, op)
		}

	}

	return fdr.getItem()
}

func (fdr *fullDocReader[T, R, J]) getItem() (*T, bool, error) {
	op := er.GetOperator()

	hasNext := fdr.rows.Next()
	if !hasNext {
		fdr.rows.Close()
		fdr.readStatus = statusFinish
		return nil, true, nil
	}

	var item T
	if err := fdr.rows.StructScan(&item); err != nil {
		log.Err(err).Msgf("scan error")
		return nil, false, er.WrapOp(err, op)
	}

	return &item, false, nil

}

func (fdr *fullDocReader[T, R, J]) read(ctx context.Context) error {
	op := er.GetOperator()

	rows, err := fdr.docReaderDB.ReadDB().QueryxContext(ctx, fdr.queryString)
	if err != nil {
		log.Err(err).Msgf("query: %s", fdr.queryString)
		return er.WrapOp(err, op)
	}

	fdr.rows = rows
	fdr.readStatus = statusDoing

	return nil
}
