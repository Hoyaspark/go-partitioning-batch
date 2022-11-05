package worker

type workerType string

var (
	consumer workerType = "consumer"
	indexer  workerType = "indexer"
)

type workerOption struct {
	isSet         bool
	workerType    workerType
	query         string
	countryCode   string
	destIndexName string
	sourceName    string
	columns       []string
}

func ConsumerWorkerOptions(readQuery, sourceName string, columns []string) workerOption {
	op := workerOption{
		isSet:      true,
		workerType: consumer,
		query:      readQuery,
		sourceName: sourceName,
		columns:    columns,
	}

	return op
}

func IndexerWorkerOptions(readQuery, destIndexName, countryCode string) workerOption {
	op := workerOption{
		isSet:         true,
		workerType:    indexer,
		query:         readQuery,
		countryCode:   countryCode,
		destIndexName: destIndexName,
	}
	return op
}
