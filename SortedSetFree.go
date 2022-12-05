package SortedSetFree

// DB agnostic SortedSets
// This is a clean implementation of Sorted Set inspired from Jerry Wang's implementation
// https://github.com/wangjia184/sortedset/blob/af6d6d227aa79e2a64b899d995ce18aa0bef437c/sortedset.go#L283
import (
	"strconv"
)

/*
	 - ZADD "key1" 23
		- ZRANG
	- SET key1 787
*/

// SortedSet is a set of keys sorted by keyrank
type SortedSet struct {
	dict           map[string]*store //This is still a hash table
	SSFreeSkiplist *SSFreeSkiplist
}

// Make makes a new SortedSet
func Make() *SortedSet {
	return &SortedSet{
		dict:           make(map[string]*store),
		SSFreeSkiplist: makeSSFreeSkiplist(),
	}
}

// Add puts key into set
func (sortedSet *SortedSet) Add(key string, keyrank float64) bool {
	element, ok := sortedSet.dict[key]
	sortedSet.dict[key] = &store{
		Key:     key,
		KeyRank: keyrank,
	}
	if ok {
		if keyrank != element.KeyRank {
			sortedSet.SSFreeSkiplist.remove(key, element.KeyRank)
			sortedSet.SSFreeSkiplist.insert(key, keyrank)
		}
		return false
	}
	sortedSet.SSFreeSkiplist.insert(key, keyrank)
	return true
}

// Len returns number of members in set
func (sortedSet *SortedSet) Len() int64 {
	return int64(len(sortedSet.dict))
}

// Get returns the given key
func (sortedSet *SortedSet) Get(key string) (element *store, ok bool) {
	element, ok = sortedSet.dict[key]
	if !ok {
		return nil, false
	}
	return element, true
}

// Remove removes the given key from set
func (sortedSet *SortedSet) Remove(key string) bool {
	v, ok := sortedSet.dict[key]
	if ok {
		sortedSet.SSFreeSkiplist.remove(key, v.KeyRank)
		delete(sortedSet.dict, key)
		return true
	}
	return false
}

// GetRank returns the rank of the given key, sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) GetRank(key string, desc bool) (rank int64) {
	element, ok := sortedSet.dict[key]
	if !ok {
		return -1
	}
	r := sortedSet.SSFreeSkiplist.getRank(key, element.KeyRank)
	if desc {
		r = sortedSet.SSFreeSkiplist.length - r
	} else {
		r--
	}
	return r
}

// ForEach visits each key which rank within [start, stop), sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) ForEach(start int64, stop int64, desc bool, consumer func(element *store) bool) {
	size := int64(sortedSet.Len())
	if start < 0 || start >= size {
		panic("illegal start " + strconv.FormatInt(start, 10))
	}
	if stop < start || stop > size {
		panic("illegal end " + strconv.FormatInt(stop, 10))
	}

	// find start node
	var node *node
	if desc {
		node = sortedSet.SSFreeSkiplist.tail
		if start > 0 {
			node = sortedSet.SSFreeSkiplist.getByRank(int64(size - start))
		}
	} else {
		node = sortedSet.SSFreeSkiplist.header.level[0].forwardNode
		if start > 0 {
			node = sortedSet.SSFreeSkiplist.getByRank(int64(start + 1))
		}
	}

	sliceSize := int(stop - start)
	for i := 0; i < sliceSize; i++ {
		if !consumer(&node.store) {
			break
		}
		if desc {
			node = node.backwardNode
		} else {
			node = node.level[0].forwardNode
		}
	}
}

// Range returns members which rank within [start, stop), sort by ascending order, rank starts from 0
func (sortedSet *SortedSet) Range(start int64, stop int64, desc bool) []*store {
	sliceSize := int(stop - start)
	slice := make([]*store, sliceSize)
	i := 0
	sortedSet.ForEach(start, stop, desc, func(element *store) bool {
		slice[i] = element
		i++
		return true
	})
	return slice
}

// Count returns the number of  members which keyrank within the given rankInterval
func (sortedSet *SortedSet) Count(min *SSFreeRankInterval, max *SSFreeRankInterval) int64 {
	var i int64 = 0
	// ascending order
	sortedSet.ForEach(0, sortedSet.Len(), false, func(element *store) bool {
		gtMin := min.less(element.KeyRank) // greater than min
		if !gtMin {
			// has not into range, continue foreach
			return true
		}
		ltMax := max.greater(element.KeyRank) // less than max
		if !ltMax {
			// break through keyrank rankInterval, break foreach
			return false
		}
		// gtMin && ltMax
		i++
		return true
	})
	return i
}

// ForEachByScore visits members which keyrank within the given rankInterval
func (sortedSet *SortedSet) ForEachByScore(min *SSFreeRankInterval, max *SSFreeRankInterval, offset int64, limit int64, desc bool, consumer func(element *store) bool) {
	// find start node
	var node *node
	if desc {
		node = sortedSet.SSFreeSkiplist.getLastInScoreRange(min, max)
	} else {
		node = sortedSet.SSFreeSkiplist.getFirstInScoreRange(min, max)
	}

	for node != nil && offset > 0 {
		if desc {
			node = node.backwardNode
		} else {
			node = node.level[0].forwardNode
		}
		offset--
	}

	// A negative limit returns all elements from the offset
	for i := 0; (i < int(limit) || limit < 0) && node != nil; i++ {
		if !consumer(&node.store) {
			break
		}
		if desc {
			node = node.backwardNode
		} else {
			node = node.level[0].forwardNode
		}
		if node == nil {
			break
		}
		gtMin := min.less(node.store.KeyRank) // greater than min
		ltMax := max.greater(node.store.KeyRank)
		if !gtMin || !ltMax {
			break // break through keyrank rankInterval
		}
	}
}

// RangeByScore returns members which keyrank within the given rankInterval
func (sortedSet *SortedSet) RangeByScore(min *SSFreeRankInterval, max *SSFreeRankInterval, offset int64, limit int64, desc bool) []*store {
	if limit == 0 || offset < 0 {
		return make([]*store, 0)
	}
	slice := make([]*store, 0)
	sortedSet.ForEachByScore(min, max, offset, limit, desc, func(element *store) bool {
		slice = append(slice, element)
		return true
	})
	return slice
}

// RemoveByScore removes members which keyrank within the given rankInterval
func (sortedSet *SortedSet) RemoveByScore(min *SSFreeRankInterval, max *SSFreeRankInterval) int64 {
	removed := sortedSet.SSFreeSkiplist.RemoveRangeByScore(min, max, 0)
	for _, element := range removed {
		delete(sortedSet.dict, element.Key)
	}
	return int64(len(removed))
}

func (sortedSet *SortedSet) PopMin(count int) []*store {
	first := sortedSet.SSFreeSkiplist.getFirstInScoreRange(NegativeInfinityExtremum, PositiveInfinityExtremum)
	if first == nil {
		return nil
	}
	rankInterval := &SSFreeRankInterval{
		Value:          first.KeyRank,
		isOpenInterval: false,
	}
	removed := sortedSet.SSFreeSkiplist.RemoveRangeByScore(rankInterval, PositiveInfinityExtremum, count)
	for _, element := range removed {
		delete(sortedSet.dict, element.Key)
	}
	return removed
}

// RemoveByRank removes key ranking within [start, stop)
// sorted by ascending order and rank starts from 0
func (sortedSet *SortedSet) RemoveByRank(start int64, stop int64) int64 {
	removed := sortedSet.SSFreeSkiplist.RemoveRangeByRank(start+1, stop+1)
	for _, element := range removed {
		delete(sortedSet.dict, element.Key)
	}
	return int64(len(removed))
}
