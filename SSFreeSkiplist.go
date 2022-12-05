package SortedSetFree

import (
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 16
)

// store is a key-keyRank pair
type store struct {
	Key     string
	KeyRank float64 //score
}

// syntax  ZADD key1 23 key1

// Level aspect of a node
type Level struct {
	forwardNode *node // forwardNode node has greater KeyRank
	span        int64
}

type node struct {
	store
	backwardNode *node
	level        []*Level
}

type SSFreeSkiplist struct {
	header *node
	tail   *node
	length int64
	level  int16
}

func makeNode(level int16, keyRank float64, key string) *node {
	n := &node{
		store: store{
			KeyRank: keyRank,
			Key:     key,
		},
		level: make([]*Level, level),
	}
	for i := range n.level {
		n.level[i] = new(Level)
	}
	return n
}

func makeSSFreeSkiplist() *SSFreeSkiplist {
	return &SSFreeSkiplist{
		level:  1,
		header: makeNode(maxLevel, 0, ""),
	}
}

func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k)) + 1
}

func (SSFreeSkiplist *SSFreeSkiplist) insert(key string, keyrank float64) *node {
	update := make([]*node, maxLevel) // link new node with node in `update`
	rank := make([]int64, maxLevel)

	// find position to insert
	node := SSFreeSkiplist.header
	for i := SSFreeSkiplist.level - 1; i >= 0; i-- {
		if i == SSFreeSkiplist.level-1 {
			rank[i] = 0
		} else {
			rank[i] = rank[i+1] // store rank that is crossed to reach the insert position
		}
		if node.level[i] != nil {
			// traverse the skip list
			for node.level[i].forwardNode != nil &&
				(node.level[i].forwardNode.KeyRank < keyrank ||
					(node.level[i].forwardNode.KeyRank == keyrank && node.level[i].forwardNode.Key < key)) { // same keyrank, different key
				rank[i] += node.level[i].span
				node = node.level[i].forwardNode
			}
		}
		update[i] = node
	}

	level := randomLevel()
	// extend SSFreeSkiplist level
	if level > SSFreeSkiplist.level {
		for i := SSFreeSkiplist.level; i < level; i++ {
			rank[i] = 0
			update[i] = SSFreeSkiplist.header
			update[i].level[i].span = SSFreeSkiplist.length
		}
		SSFreeSkiplist.level = level
	}

	// make node and link into SSFreeSkiplist
	node = makeNode(level, keyrank, key)
	for i := int16(0); i < level; i++ {
		node.level[i].forwardNode = update[i].level[i].forwardNode
		update[i].level[i].forwardNode = node

		// update span covered by update[i] as node is inserted here
		node.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = (rank[0] - rank[i]) + 1
	}

	// increment span for untouched levels
	for i := level; i < SSFreeSkiplist.level; i++ {
		update[i].level[i].span++
	}

	// set backwardNode node
	if update[0] == SSFreeSkiplist.header {
		node.backwardNode = nil
	} else {
		node.backwardNode = update[0]
	}
	if node.level[0].forwardNode != nil {
		node.level[0].forwardNode.backwardNode = node
	} else {
		SSFreeSkiplist.tail = node
	}
	SSFreeSkiplist.length++
	return node
}

func (SSFreeSkiplist *SSFreeSkiplist) removeNode(node *node, update []*node) {
	for i := int16(0); i < SSFreeSkiplist.level; i++ {
		if update[i].level[i].forwardNode == node {
			update[i].level[i].span += node.level[i].span - 1
			update[i].level[i].forwardNode = node.level[i].forwardNode
		} else {
			update[i].level[i].span--
		}
	}
	if node.level[0].forwardNode != nil {
		node.level[0].forwardNode.backwardNode = node.backwardNode
	} else {
		SSFreeSkiplist.tail = node.backwardNode
	}
	for SSFreeSkiplist.level > 1 && SSFreeSkiplist.header.level[SSFreeSkiplist.level-1].forwardNode == nil {
		SSFreeSkiplist.level--
	}
	SSFreeSkiplist.length--
}

func (SSFreeSkiplist *SSFreeSkiplist) remove(key string, keyrank float64) bool {
	/*
	 * find backwardNode node (of target) or last node of each level
	 * their forwardNode need to be updated
	 */
	update := make([]*node, maxLevel)
	node := SSFreeSkiplist.header
	for i := SSFreeSkiplist.level - 1; i >= 0; i-- {
		for node.level[i].forwardNode != nil &&
			(node.level[i].forwardNode.KeyRank < keyrank ||
				(node.level[i].forwardNode.KeyRank == keyrank &&
					node.level[i].forwardNode.Key < key)) {
			node = node.level[i].forwardNode
		}
		update[i] = node
	}
	node = node.level[0].forwardNode
	if node != nil && keyrank == node.KeyRank && node.Key == key {
		SSFreeSkiplist.removeNode(node, update)
		// free x
		return true
	}
	return false
}

func (SSFreeSkiplist *SSFreeSkiplist) getRank(key string, keyrank float64) int64 {
	var rank int64 = 0
	x := SSFreeSkiplist.header
	for i := SSFreeSkiplist.level - 1; i >= 0; i-- {
		for x.level[i].forwardNode != nil &&
			(x.level[i].forwardNode.KeyRank < keyrank ||
				(x.level[i].forwardNode.KeyRank == keyrank &&
					x.level[i].forwardNode.Key <= key)) {
			rank += x.level[i].span
			x = x.level[i].forwardNode
		}

		/* x might be equal to zsl->header, so test if obj is non-NULL */
		if x.Key == key {
			return rank
		}
	}
	return 0
}

func (SSFreeSkiplist *SSFreeSkiplist) getByRank(rank int64) *node {
	var i int64 = 0
	n := SSFreeSkiplist.header
	// scan from top level
	for level := SSFreeSkiplist.level - 1; level >= 0; level-- {
		for n.level[level].forwardNode != nil && (i+n.level[level].span) <= rank {
			i += n.level[level].span
			n = n.level[level].forwardNode
		}
		if i == rank {
			return n
		}
	}
	return nil
}

func (SSFreeSkiplist *SSFreeSkiplist) hasInRange(min *SSFreeRankInterval, max *SSFreeRankInterval) bool {
	// min & max = empty
	if min.Value > max.Value || (min.Value == max.Value && (min.isOpenInterval || max.isOpenInterval)) {
		return false
	}
	// min > tail
	n := SSFreeSkiplist.tail
	if n == nil || !min.less(n.KeyRank) {
		return false
	}
	// max < head
	n = SSFreeSkiplist.header.level[0].forwardNode
	if n == nil || !max.greater(n.KeyRank) {
		return false
	}
	return true
}

func (SSFreeSkiplist *SSFreeSkiplist) getFirstInScoreRange(min *SSFreeRankInterval, max *SSFreeRankInterval) *node {
	if !SSFreeSkiplist.hasInRange(min, max) {
		return nil
	}
	n := SSFreeSkiplist.header
	// scan from top level
	for level := SSFreeSkiplist.level - 1; level >= 0; level-- {
		// if forwardNode is not in range than move forwardNode
		for n.level[level].forwardNode != nil && !min.less(n.level[level].forwardNode.KeyRank) {
			n = n.level[level].forwardNode
		}
	}
	/* This is an inner range, so the next node cannot be NULL. */
	n = n.level[0].forwardNode
	if !max.greater(n.KeyRank) {
		return nil
	}
	return n
}

func (SSFreeSkiplist *SSFreeSkiplist) getLastInScoreRange(min *SSFreeRankInterval, max *SSFreeRankInterval) *node {
	if !SSFreeSkiplist.hasInRange(min, max) {
		return nil
	}
	n := SSFreeSkiplist.header
	// scan from top level
	for level := SSFreeSkiplist.level - 1; level >= 0; level-- {
		for n.level[level].forwardNode != nil && max.greater(n.level[level].forwardNode.KeyRank) {
			n = n.level[level].forwardNode
		}
	}
	if !min.less(n.KeyRank) {
		return nil
	}
	return n
}

func (SSFreeSkiplist *SSFreeSkiplist) RemoveRangeByScore(min *SSFreeRankInterval, max *SSFreeRankInterval, limit int) (removed []*store) {
	update := make([]*node, maxLevel)
	removed = make([]*store, 0)
	// find backwardNode nodes (of target range) or last node of each level
	node := SSFreeSkiplist.header
	for i := SSFreeSkiplist.level - 1; i >= 0; i-- {
		for node.level[i].forwardNode != nil {
			if min.less(node.level[i].forwardNode.KeyRank) { // already in range
				break
			}
			node = node.level[i].forwardNode
		}
		update[i] = node
	}

	// node is the first one within range
	node = node.level[0].forwardNode

	// remove nodes in range
	for node != nil {
		if !max.greater(node.KeyRank) { // already out of range
			break
		}
		next := node.level[0].forwardNode
		removedstore := node.store
		removed = append(removed, &removedstore)
		SSFreeSkiplist.removeNode(node, update)
		if limit > 0 && len(removed) == limit {
			break
		}
		node = next
	}
	return removed
}

func (SSFreeSkiplist *SSFreeSkiplist) RemoveRangeByRank(start int64, stop int64) (removed []*store) {
	var i int64 = 0 // rank of iterator
	update := make([]*node, maxLevel)
	removed = make([]*store, 0)

	// scan from top level
	node := SSFreeSkiplist.header
	for level := SSFreeSkiplist.level - 1; level >= 0; level-- {
		for node.level[level].forwardNode != nil && (i+node.level[level].span) < start {
			i += node.level[level].span
			node = node.level[level].forwardNode
		}
		update[level] = node
	}

	i++
	node = node.level[0].forwardNode // first node in range

	// remove nodes in range
	for node != nil && i < stop {
		next := node.level[0].forwardNode
		removedstore := node.store
		removed = append(removed, &removedstore)
		SSFreeSkiplist.removeNode(node, update)
		node = next
		i++
	}
	return removed
}
