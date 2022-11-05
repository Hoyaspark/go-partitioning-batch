package parallel

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"testing"
)

func Test_SortableID(t *testing.T) {
	t.Run("divide with sortable id", func(t *testing.T) {
		pm := NewParallel("", &parallelDBMock{
			count: 3000,
		}, 10)

		result, err := pm.Partition(SortableId)

		assert.NoError(t, err)

		mockData := []mockSortableIDData{
			{301, 0},
			{301, 301},
			{301, 602},
			{301, 903},
			{301, 1204},
			{301, 1505},
			{301, 1806},
			{301, 2107},
			{301, 2408},
			{301, 2709},
		}

		for i, r := range result {
			assert.Equal(t, r.Min(), mockData[i].limit)
			assert.Equal(t, r.Max(), mockData[i].offset)
		}
	})
}

func Test_AutoIncrementID(t *testing.T) {
	t.Run("divide with auto increment id", func(t *testing.T) {
		pm := NewParallel("", &parallelDBMock{
			min: 1,
			max: 94858,
		}, 10)

		result, err := pm.Partition(AutoIncrementId)

		assert.NoError(t, err)

		mockData := []mockAutoIncrementIDData{
			{1, 9486},
			{9487, 18972},
			{18973, 28458},
			{28459, 37944},
			{37945, 47430},
			{47431, 56916},
			{56917, 66402},
			{66403, 75888},
			{75889, 85374},
			{85375, 94858},
		}

		for i, r := range result {
			assert.Equal(t, r.Min(), mockData[i].min)
			assert.Equal(t, r.Max(), mockData[i].max)
		}
	})
}

type parallelDBMock struct {
	mock.Mock
	count int64
	min   int64
	max   int64
}

func (m *parallelDBMock) GetSortBy(sourceName string) (Partition, error) {
	return NewPartition(m.min, m.max, m.count), nil
}

type mockSortableIDData struct {
	limit  int64
	offset int64
}

type mockAutoIncrementIDData struct {
	min int64
	max int64
}
