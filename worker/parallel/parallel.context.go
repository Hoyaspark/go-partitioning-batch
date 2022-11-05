package parallel

const (
	AutoIncrementIdType int64 = iota
	SortableIdType
)

var EmptyPartition = NewPartition(0, 0, 0)

type partition struct {
	parallelId    string
	min           int64
	max           int64
	count         int64
	partitionType int64
}

func NewPartition(min, max, count int64) Partition {
	return &partition{
		min:   min,
		max:   max,
		count: count,
	}
}

func (p *partition) PartitionName() string {
	return p.parallelId
}

func (p *partition) Min() int64 {
	return p.min
}

func (p *partition) Max() int64 {
	return p.max
}

func (p *partition) Count() int64 {
	return p.count
}

func (p *partition) Type() int64 {
	return p.partitionType
}
