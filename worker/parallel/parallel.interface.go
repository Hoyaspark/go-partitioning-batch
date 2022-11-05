package parallel

type Parallel interface {
	Partition(ParallelTypeFunc) ([]Partition, error)
}

type Partition interface {
	PartitionName() string
	Min() int64
	Max() int64
	Count() int64
	Type() int64
}

type ParallelDB interface {
	GetSortBy(string) (Partition, error)
}
