package writer

import (
	"github.com/Hoyaspark/go-partitioning-batch/pkg/er"
	"github.com/Hoyaspark/go-partitioning-batch/util"
	"github.com/Hoyaspark/go-partitioning-batch/worker/parallel"
	"github.com/Hoyaspark/go-partitioning-batch/worker/step"
	"github.com/fatih/structs"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"time"
)

type sqlxDocWriter[R, K any] struct {
	queryString      string // need to set "INSERT INTO ~~~"
	docWriterStorage step.WriterStorage[K]
}

func NewSqlxDocWriter[R, K any](queryString string, db step.WriterStorage[K]) step.Writer[R, K] {
	return &sqlxDocWriter[R, K]{
		queryString:      queryString,
		docWriterStorage: db,
	}
}

func (sdw *sqlxDocWriter[R, K]) Write(items []R, pCtx parallel.Partition) (int64, error) {
	var rowsAffCount int64

	op := er.GetOperator()

	db, ok := any(sdw.docWriterStorage.GetClient()).(*sqlx.DB)
	if !ok {
		return 0, er.WrapOp(errors.New("cannot convert docWriterStorage client"), op)
	}

	err := util.RetryFunc(2, 100*time.Millisecond, func() (err error) {
		var buf []map[string]interface{}

		for _, item := range items {
			buf = append(buf, structs.Map(item))
		}

		rowsRes, err := db.NamedExec(sdw.queryString, buf)
		if err != nil {
			return err
		}

		rowsAff, err := rowsRes.RowsAffected()
		if err != nil {
			return err
		}

		rowsAffCount += rowsAff

		return
	})
	if err != nil {
		return 0, er.WrapOp(err, op)
	}

	return rowsAffCount, nil
}
