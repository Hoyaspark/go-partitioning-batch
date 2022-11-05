package step

const (
	defaultConsumerPageSize  int64 = 200000
	defaultConsumerChunkSize       = 300
	defaultIndexerPageSize         = 2000000
	defaultIndexerChunkSize        = 10000

	FullRead readerType = iota
	PagingRead
)

type readerType int64

type stepOption struct {
	pageSize   int64
	chunkSize  int64
	readerType readerType
}

func (so *stepOption) PageSize() int64 {
	return so.pageSize
}

func (so *stepOption) ChunkSize() int64 {
	return so.chunkSize
}

func (so *stepOption) ReaderType() readerType {
	return so.readerType
}

var (
	DefaultPagingConsumerOption Option = &defaultStepOptions{
		readerType: PagingRead,
		pageSize:   defaultConsumerPageSize,
		chunkSize:  defaultConsumerChunkSize,
	}

	DefaultPagingIndexerOption Option = &defaultStepOptions{
		readerType: PagingRead,
		pageSize:   defaultIndexerPageSize,
		chunkSize:  defaultIndexerChunkSize,
	}

	DefaultFullConsumerOption Option = &defaultStepOptions{
		readerType: FullRead,
		chunkSize:  defaultConsumerChunkSize,
	}

	DefaultFullIndexerOption Option = &defaultStepOptions{
		readerType: FullRead,
		chunkSize:  defaultIndexerChunkSize,
	}

	ArticleConsumerOption Option = &defaultStepOptions{
		readerType: PagingRead,
		pageSize:   defaultConsumerPageSize,
		chunkSize:  defaultConsumerChunkSize,
	}
)

type defaultStepOptions stepOption

func (dso *defaultStepOptions) PageSize() int64 {
	return dso.pageSize
}

func (dso *defaultStepOptions) ChunkSize() int64 {
	return dso.chunkSize
}

func (dso *defaultStepOptions) ReaderType() readerType {
	return dso.readerType
}
