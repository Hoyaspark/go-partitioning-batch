package monitoring

import "sync/atomic"

type rowCountLog struct {
	contextName      string
	rowAffectedCount int64
	rowCount         int64
}

func (rcl *rowCountLog) ContextName() string {
	return rcl.contextName
}

func (rcl *rowCountLog) RowAffectedCount() int64 {
	return rcl.rowAffectedCount
}

func (rcl *rowCountLog) RowCount() int64 {
	return rcl.rowCount
}

func NewRowCountLog(contextName string, rowAffectedCount, rowCount int64) RowCountLog {
	return &rowCountLog{
		contextName:      contextName,
		rowAffectedCount: rowAffectedCount,
		rowCount:         rowCount,
	}
}

type monitoring struct {
	sendingErrCh    chan error
	rowCountMonitor chan RowCountLog
	runningCount    int64
}

func NewMonitoring(len int) WorkerMonitoring {

	return &monitoring{
		sendingErrCh:    make(chan error),
		rowCountMonitor: make(chan RowCountLog, len),
		runningCount:    int64(len),
	}
}

func (m *monitoring) Send(cnt RowCountLog) {
	m.rowCountMonitor <- cnt
}

func (m *monitoring) SendErr(err error) {
	m.sendingErrCh <- err
}

func (m *monitoring) ReceiveErr() <-chan error {
	return m.sendingErrCh
}
func (m *monitoring) ReceiveResult() <-chan RowCountLog {
	return m.rowCountMonitor
}

func (m *monitoring) IsFinish() bool {
	if m.runningCount < 1 {
		return true
	}

	return false
}

func (m *monitoring) Finish() {
	atomic.AddInt64(&m.runningCount, -1)
}
