// This package creates a skiplist implementation of a concurrent safe index that can store and access ordered key value pairs
package skipList

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// A node is an element within the skiplist containing one key value pair. A zero value node is an empty node ready to use.
type node[K cmp.Ordered, V any] struct {
	key         K
	value       V
	mtx         sync.Mutex
	topLevel    int
	marked      bool
	fullyLinked bool
	time        time.Time
	next        []atomic.Pointer[node[K, V]]
}

// A function that takes in a key, a current value (if it exists) and outputs what the new value to store should be when
// updating or inserting.
type UpdateCheck[K cmp.Ordered, V any] func(key K, currValue V, exists bool) (newValue V, err error)

// This function operates on a node and takes another node as a parameter. Returns true if the two nodes have the same keys and creation times, false otherwise
func (n *node[K, V]) equals(toCompare *node[K, V]) bool {
	if (n.key == toCompare.key) && (n.time == toCompare.time) {
		slog.Info(fmt.Sprintf("found nodes to be equal: %v and %v", n.key, toCompare.key))
		return true
	} else {
		slog.Info(fmt.Sprintf("found nodes to be unequal: %v and %v", n.key, toCompare.key))
		return false
	}
}

// A SkipList is a concurrent safe index that can store and access ordered key value pairs. A SkipList should be created using the New function
type Skiplist[K cmp.Ordered, V any] struct {
	head *node[K, V]
}

// New function creates a skiplist with a head node (with the provided min value key) pointing to the tail (with provided maximum value key) node at every level.
func New[K cmp.Ordered, V any](name string, minVal K, maxVal K) *Skiplist[K, V] {

	tail := new(node[K, V])
	tail.key = maxVal
	tail.topLevel = 0
	tail.marked = false
	tail.fullyLinked = true

	head := new(node[K, V])
	head.key = minVal
	head.topLevel = 0
	head.marked = false
	head.fullyLinked = true
	head.next = make([]atomic.Pointer[node[K, V]], 5)
	head.next[0].Store(tail)
	head.next[1].Store(tail)
	head.next[2].Store(tail)
	head.next[3].Store(tail)
	head.next[4].Store(tail)
	slog.Info(fmt.Sprintf("created new skip list with name %s", name))
	return &Skiplist[K, V]{head: head}
}

// This methods searches a skiplist for a node with the given key, and returns the highest level it is found at
// a list of predecessors that point to this node (up to this highest level) or an item with a key greater than the provided key
// if the provided key is not foound. It also returns a list of sucessors which these predecesors point to.
func (s *Skiplist[K, V]) find(key K) (int, []*node[K, V], []*node[K, V]) {
	foundLevel := -1
	preds := make([]*node[K, V], len(s.head.next))
	succs := make([]*node[K, V], len(s.head.next))

	pred := s.head
	level := len(s.head.next) - 1

	for level >= 0 {
		curr := pred.next[level].Load()
		for key > curr.key {
			pred = curr
			curr = pred.next[level].Load()
		}
		if foundLevel == -1 && key == curr.key {
			foundLevel = level
			slog.Info(fmt.Sprintf("found %v at level %d", key, level))
		}

		preds[level] = pred
		succs[level] = curr
		level = level - 1
	}

	if level == -1 {
		slog.Info(fmt.Sprintf("didnt find key %v", key))
	}
	return foundLevel, preds, succs
}

// This method finds a key, and returns its corresponding value if it exists, otherwise a zero value of the value type. Also returns
// a boolean corresponding to if the key is found
func (s *Skiplist[K, V]) Find(key K) (V, bool) {
	levelFound, _, succs := s.find(key)
	ok := (levelFound != -1)
	if !ok {
		var none V
		slog.Info(fmt.Sprintf("didnt find key %v ", key))
		return none, false
	} else {
		foundNode := succs[levelFound]
		if foundNode.marked || !foundNode.fullyLinked {
			var none V
			return none, false
		}
		return foundNode.value, ok
	}

}

// Functionally identical to upsert, but takes input of func(key K, currValue V, exists bool) (V, error) rather than
// checkfunction. Calls upsert with this function as a check function
func (s *Skiplist[K, V]) CallUpsert(key K, check func(key K, currValue V, exists bool) (V, error)) (V, error) {
	return s.Upsert(key, check)
}

// Upsert takes a key and a updatecheck function. If they key is in the skiplist, it will lock the node with that key,
// check if the key is being deleted or inserted, and call the check function with the found value
// If the key is not found, it will call the check and insert the returned value into the skiplist
func (s *Skiplist[K, V]) Upsert(key K, check UpdateCheck[K, V]) (V, error) {

	// Pick random top level
	topLevel := randomLevel(len(s.head.next) - 2)
	slog.Info(fmt.Sprintf("chose top level %d for key %v", topLevel, key))
	// Keep trying to insert until success/failure
	for {

		lockMap := make(map[*node[K, V]]bool)
		levelFound, preds, succs := s.find(key)
		if levelFound != -1 {
			slog.Info(fmt.Sprintf("found existing key %v during upsert", key))
			found := succs[levelFound]

			if !found.marked {
				// Node is being added, wait for other insert to finish
				for !found.fullyLinked {
				}

				found.mtx.Lock()
				slog.Info(fmt.Sprintf("locked existing key %v during upsert", key))
				if !found.marked && found.fullyLinked {
					found.time = time.Now()
					// Did not insert this key/value pair
					toPut, err := check(key, found.value, true)
					// if err == nil{
					// 	found.value = toPut
					// }
					slog.Info(fmt.Sprintf("modified exising node with key %v to have value %v", key, toPut))
					found.mtx.Unlock()

					return toPut, err
				}
				found.mtx.Unlock()

			}
			// Found node is being removed, try again
			slog.Info(fmt.Sprintf("unlocked key %v during upsert", key))
			continue
		} else {
			highestLocked := -1
			valid := true
			level := 0
			// Lock all predecessors
			for valid && level <= topLevel {
				isLocked, ok := lockMap[preds[level]]
				slog.Info(fmt.Sprintf("predecessor to key %v at level %d is already locked?: %t", key, level, (ok && isLocked)))
				if !ok || !isLocked {
					preds[level].mtx.Lock()
					slog.Info(fmt.Sprintf("locked predecessor to key %v at level %d", key, level))
					lockMap[preds[level]] = true
				}
				highestLocked = level
				// Check if pred/succ are still valid
				unmarked := (!preds[level].marked && !succs[level].marked)
				connected := preds[level].next[level].Load().equals(succs[level])
				valid = unmarked && connected
				level = level + 1
			}
			if !valid {
				// Predecessors or successors changed,
				// unlock and try again
				level = highestLocked
				for level >= 0 {
					isLocked, ok := lockMap[preds[level]]
					if ok && isLocked {
						preds[level].mtx.Unlock()
						slog.Info(fmt.Sprintf("unlocked predecessor to key %v at level %d", key, level))
						lockMap[preds[level]] = false
					}
					level = level - 1
				}
				continue
			}
			var empty V
			value, err := check(key, empty, false)
			if err != nil {
				slog.Error(err.Error())
				return empty, err
			}

			node := node[K, V]{key: key, value: value, topLevel: topLevel, marked: false, fullyLinked: false, time: time.Now(), next: make([]atomic.Pointer[node[K, V]], (topLevel + 1))}
			slog.Info(fmt.Sprintf("created new node with key %v and value %v", key, value))
			// Set next pointers
			level = 0

			for level <= topLevel {
				node.next[level].Store(succs[level])
				level = level + 1
			}
			// Add to skip list from bottom up
			level = 0
			for level <= topLevel {
				preds[level].next[level].Store(&node)
				level = level + 1
			}

			node.fullyLinked = true
			slog.Info(fmt.Sprintf("new node with key %v fully linked", key))
			// Unlock
			level = highestLocked
			for level >= 0 {
				isLocked, ok := lockMap[preds[level]]
				if ok && isLocked {
					preds[level].mtx.Unlock()
					slog.Info(fmt.Sprintf("unlocked predecessor to key %v at level %d", key, level))
					lockMap[preds[level]] = false
				}
				level = level - 1
			}
			return value, nil
		}
	}
}

// Query takes a context and a starting key value and and ending key value, and returns a list of keys and a list of corresponding values from within the skiplist with keys between the start and end
// values (inclusive). Ensures concurrent saftey by iterating over the list twice and ensuring it finds the same nodes (with the same keys and last modified times) in both iterattions
// If iterations don't match, retries, stopping if the context Deadline passes.
func (s *Skiplist[K, V]) Query(ctx context.Context, start K, end K, copier func(val V) any) (resultKeys []K, resultValues []V, err error) {
	toLog := ""
	ctxNil := (ctx == nil)
	var giveUpTime time.Time
	var ok bool
	if !ctxNil {
		giveUpTime, ok = ctx.Deadline()
	}
	for ctxNil || !ok || !time.Now().After(giveUpTime) || ctx.Err() == nil {
		curr := s.head
		first_iter := make([]*node[K, V], 0)
		toReturnKeys := make([]K, 0)
		toReturnValues := make([]V, 0)
		toLog += ("\n First Iteration: ")
		tail := s.head.next[len(s.head.next)-1].Load()
		next := curr.next[0].Load()
		for !next.equals(tail) && next.key >= start && next.key <= end {
			curr = next
			if !curr.marked {
				first_iter = append(first_iter, curr)
				toReturnKeys = append(toReturnKeys, curr.key)
				copy := copier(curr.value)
				if copy == nil {
					slog.Error("couldn't copy value in query")
					return nil, nil, errors.New(`"couldn't copy value in query"`)
				}
				copied, ok := copy.(V)
				if !ok {
					slog.Error("couldn't copy value in query")
					return nil, nil, errors.New(`"couldn't copy value in query"`)
				}
				toReturnValues = append(toReturnValues, copied)
				toLog += fmt.Sprint(curr.key)
				toLog += (", ")
			}
			next = curr.next[0].Load()
		}
		toLog += ("\n Onto Second Iteration: ")

		allOk := true
		curr = s.head
		i := 0
		tail = s.head.next[len(s.head.next)-1].Load()
		next = curr.next[0].Load()
		for !next.equals(tail) && next.key >= start && next.key <= end && i < len(first_iter) && allOk {
			curr = next
			if first_iter[i].equals(curr) && !curr.marked {
				toLog += fmt.Sprint(curr.key)
				toLog += (", ")

			} else {
				allOk = false
			}
			i++
			next = curr.next[0].Load()
		}
		if allOk {
			toLog += ("\nRETURNING\n")
			slog.Debug(toLog)
			return toReturnKeys, toReturnValues, nil
		}
	}
	slog.Error("deadline past during query or context done")
	return nil, nil, errors.New(`"deadline past durying query or context done"`)
}

// Remove takes a key value and removes the node with this key from the skipList if it exists. Returns the value corresponding
// to this key if it was removed and a boolean representing whether or not a node was succesfully removed.
func (s *Skiplist[K, V]) Remove(key K) (V, bool) {
	lockMap := make(map[*node[K, V]]bool)
	var victim *node[K, V] // Victim node to remove
	isMarked := false      // Have we already marked the victim?
	topLevel := -1         // Top level of victim node

	// Find victim (or fail), lock and mark it on first iteration

	// Keep trying to remove until success/failure
	for {
		levelFound, preds, succs := s.find(key)
		if levelFound != -1 {
			victim = succs[levelFound]
			slog.Info(fmt.Sprintf("victim with key %v found for removal", key))
		}
		if !isMarked {
			// First time through
			var empty V
			if levelFound == -1 {
				slog.Info(fmt.Sprintf("No node found with key %v", key))
				return empty, false
			}
			if !victim.fullyLinked {
				slog.Info(fmt.Sprintf("victim with key %v still being inserted", key))
				return empty, false
			}

			if victim.marked {
				slog.Info(fmt.Sprintf("victim with key %v already marked for deletion", key))
				return empty, false
			}
			if victim.topLevel != levelFound {
				slog.Info(fmt.Sprintf("victim with key %v not fully linked", key))
				return empty, false
			}
			topLevel = victim.topLevel
			victim.mtx.Lock()
			if victim.marked {
				// Another remove call beat us
				victim.mtx.Unlock()
				return empty, false
			}
			victim.marked = true
			isMarked = true
			slog.Info(fmt.Sprintf("victim with key %v marked for deletion", key))
		}

		// Victim is locked and marked
		highestLocked := -1
		level := 0
		valid := true
		for valid && (level <= topLevel) {
			pred := preds[level]
			isLocked, ok := lockMap[preds[level]]
			slog.Info(fmt.Sprintf("predecessor to key %v at level %d is already locked?: %t", key, level, (ok && isLocked)))
			if !ok || !isLocked {
				preds[level].mtx.Lock()
				slog.Info(fmt.Sprintf("locked predecessor to key %v at level %d", key, level))
				lockMap[preds[level]] = true
			}
			highestLocked = level
			successor := pred.next[level].Load().equals(victim)
			valid = !pred.marked && successor
			level = level + 1
		}

		if !valid {
			// Unlock
			level = highestLocked
			for level >= 0 {
				isLocked, ok := lockMap[preds[level]]
				slog.Info(fmt.Sprintf("predecessor to key %v at level %d is already locked?: %t", key, level, (ok && isLocked)))
				if ok && isLocked {
					preds[level].mtx.Unlock()
					slog.Info(fmt.Sprintf("unlocked predecessor to key %v at level %d", key, level))
					lockMap[preds[level]] = false
				}
				level = level - 1
			}
			continue
		}

		level = topLevel
		for level >= 0 {
			preds[level].next[level].Store(victim.next[level].Load())
			slog.Info(fmt.Sprintf("predecessor to victim with key %v at level %d no longer points to victim", key, level))
			level = level - 1
		}
		// Unlock
		victim.mtx.Unlock()
		slog.Info(fmt.Sprintf("victim with key %v unlocked", key))
		level = highestLocked
		for level >= 0 {
			isLocked, ok := lockMap[preds[level]]
			slog.Info(fmt.Sprintf("predecessor to key %v at level %d is already locked?: %t", key, level, (ok && isLocked)))
			if ok && isLocked {
				preds[level].mtx.Unlock()
				slog.Info(fmt.Sprintf("unlocked predecessor to key %v at level %d", key, level))
				lockMap[preds[level]] = false
			}
			level = level - 1
		}
		return victim.value, true
	}
}

// Takes an int upTo, returns an int n with the probability of returning any int being 0.5^(n + 1) for n < upTo.
// The remaining probability returns upTo.
func randomLevel(upTo int) int {

	random := rand.Float64()
	n := 0
	for n < upTo {
		if random > math.Pow(0.5, float64(n+1)) {
			return n
		}
		n++
	}
	return upTo

}
