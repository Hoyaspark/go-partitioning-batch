package processor

import (
	"go-partitioning-batch/pkg/er"
	"go-partitioning-batch/worker/parallel"
	"go-partitioning-batch/worker/step"
)

type processor[T step.DocProcessor[R, J], R any, J step.ProcessorParam[J]] struct {
}

func NewProcessor[T step.DocProcessor[R, J], R any, J step.ProcessorParam[J]]() step.Processor[T, R, J] {
	return &processor[T, R, J]{}
}

func (p *processor[T, R, J]) Process(item T, param *J, pCtx parallel.Partition) (*R, error) {
	op := er.GetOperator()

	refineItem, err := item.ToModel(param)
	if err != nil {
		return nil, er.WrapOp(err, op)
	}

	return refineItem, nil
}
