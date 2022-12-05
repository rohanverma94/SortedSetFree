package SortedSetFree

// Ideal to implement Redis Commands based on ZADD key [GT | LT] keyRank key
const (
	NegativeInfinity int8 = -1
	PositiveInfinity int8 = 1
)

// SSFreeRankInterval represents range of a float value, including: <, <=, >, >=, +inf, -inf
type SSFreeRankInterval struct {
	limit          int8
	Value          float64
	isOpenInterval bool
}

func (rankInterval *SSFreeRankInterval) greater(value float64) bool {
	switch rankInterval.limit {
	case NegativeInfinity:
		return false
	case PositiveInfinity:
		return true
	default:
		if rankInterval.isOpenInterval {
			return rankInterval.Value > value
		}
		return rankInterval.Value >= value
	}
}

func (rankInterval *SSFreeRankInterval) less(value float64) bool {
	switch rankInterval.limit {
	case NegativeInfinity:
		return true
	case PositiveInfinity:
		return false
	default:
		if rankInterval.isOpenInterval {
			return rankInterval.Value < value
		}
		return rankInterval.Value <= value
	}
}

var PositiveInfinityExtremum = &SSFreeRankInterval{
	limit: PositiveInfinity,
}

var NegativeInfinityExtremum = &SSFreeRankInterval{
	limit: NegativeInfinity,
}
