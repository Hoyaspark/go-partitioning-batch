package reader

type status int64

const (
	statusReady status = iota
	statusDoing
	statusFinish
)
