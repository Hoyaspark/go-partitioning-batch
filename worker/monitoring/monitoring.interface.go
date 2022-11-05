package monitoring

type WorkerMonitoring interface {
	Send(cnt RowCountLog)
	SendErr(err error)
	ReceiveResult() <-chan RowCountLog
	ReceiveErr() <-chan error
	IsFinish() bool
	Finish()
}

type RowCountLog interface {
	ContextName() string
	RowAffectedCount() int64
	RowCount() int64
}
