package skipList

import (
	"context"
	"io"
	"log"
	"slices"
	"strconv"
	"sync"
	"testing"
)

func TestSkipList(t *testing.T) {

	log.SetOutput(io.Discard)

	//FEEDING TO RANDOM
	// oneSlice := []int{3, 1, 2, 1, 1, 1, 2, 0, 0, 1, 3, 0, 0, 0, 1, 3, 2, 2, 0, 0, 2, 0, 2, 1, 1, 1, 0, 1, 0, 0, 0, 2, 2, 0, 0, 0, 3, 0, 3, 0, 0, 1, 1, 1, 1, 0, 0, 0, 0, 1, 2, 0, 0, 1, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 1, 3, 1, 0, 0, 0, 3, 1, 0, 1, 2, 0, 0, 0, 0, 0, 1, 0, 3, 3, 2, 1, 3, 2, 3, 3, 1, 2, 1, 0, 0, 2, 0, 0, 1, 3, 0, 1, 2, 1, 3, 3, 2, 0, 2, 2, 1, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 2, 3, 3, 2, 3, 3, 2, 0, 0, 1, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 3, 3, 1, 0, 0, 2, 1, 3, 3, 0, 2, 1, 2, 0, 2, 1, 0, 3, 1, 0, 2, 1, 1, 3, 0, 1, 0, 2, 2, 0, 3, 0, 0, 0, 0, 0, 0, 1, 1, 2, 3, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 3, 1, 0, 0, 3, 2, 0, 2, 3, 0, 1, 0, 0, 3, 3, 2, 1, 2, 0, 0, 1, 0, 1, 2, 0, 0, 2, 0, 0, 0, 0, 1, 0, 0, 2, 0, 0, 0, 1, 3, 0, 0, 1, 0, 0, 0, 2, 0, 2, 0, 0, 1, 1, 0, 0, 1, 1, 2, 1, 0, 3, 0, 0, 1, 1, 1, 1, 0, 1, 0, 0, 2, 3, 0, 3, 1, 1, 0, 2, 1, 0, 1, 0, 0, 2, 1, 0, 0, 0, 3, 0, 0, 0, 0, 1, 1, 0, 2, 2, 0, 0, 0, 3, 1, 0, 0, 0, 0, 3, 0, 0, 1, 0, 0, 0, 2, 3, 3, 2, 0, 0, 0, 2, 0, 3, 0, 0, 0, 3, 3, 0, 0, 2, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 2, 0, 0, 1, 2, 1, 0, 0, 2, 2, 2, 1, 3, 1, 3, 0, 0, 2, 0, 3, 3, 0, 0, 0, 2, 0, 0, 2, 0, 0, 0, 1, 0, 1, 2, 0, 0, 1, 2, 1, 0, 0, 2, 1, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 2, 3, 0, 2, 3, 1, 2, 1, 1, 1, 0, 1, 1, 1, 1, 0, 0, 2, 0, 3, 0, 2, 0, 1, 0, 1, 2, 2, 0, 0, 0, 0, 1, 0, 0, 0, 0, 3, 0, 1, 0, 0, 1, 0, 1, 0, 3, 0, 0, 1, 1, 0, 1, 0, 1, 0, 0, 0, 3, 0, 3, 0, 1, 0, 2, 0, 1, 0, 0, 1, 0, 1, 0, 0, 3, 1, 0, 0, 1, 3, 2, 0, 0, 0, 0, 3, 2, 1, 0, 0, 2, 1, 2, 3, 2, 1, 3, 2, 0, 0, 3, 0, 3, 3, 1, 0, 1, 2, 0, 3, 0, 1, 1, 3, 0, 0, 0, 1, 3, 0, 0, 1, 0, 1, 3, 2, 0, 0, 0, 1, 1, 1, 0, 3, 0, 1, 2, 0, 0, 2, 1, 3, 0, 1, 0, 0, 2, 0, 1, 2, 0, 0, 0, 2, 1, 2, 1, 0, 0, 0, 2, 1, 1, 1, 0, 0, 0, 1, 1, 0, 0, 0, 2, 0, 3, 1, 0, 1, 0, 0, 1, 0, 3, 3, 1, 0, 0, 1, 3, 1, 1, 0, 1, 1, 0, 0, 0, 1, 1, 3, 2, 0, 1, 0, 2, 0, 2, 0, 3, 0, 1, 0, 0, 0, 0, 1, 0, 1, 1, 2, 1, 0, 1, 0, 0, 1, 1, 3, 1, 0, 1, 0, 3, 0, 0, 0, 0, 0, 0, 3, 2, 3, 0, 0, 3, 0, 0, 1, 0, 1, 3, 0, 0, 1, 3, 0, 1, 0, 2, 1, 2, 1, 3, 1, 0, 0, 1, 3, 0, 3, 0, 0, 0, 3, 0, 0, 2, 0, 1, 1, 1, 2, 3, 0, 2, 0, 1, 1, 1, 0, 0, 1, 2, 3, 3, 1, 0, 0, 2, 0, 0, 1, 0, 0, 2, 0, 0, 0, 3, 3, 0, 3, 1, 2, 0, 3, 1, 0, 0, 0, 1, 0, 1, 0, 0, 0, 3, 3, 3, 0, 1, 3, 0, 0, 2, 1, 3, 1, 2, 0, 3, 1, 0, 0, 2, 0, 2, 3, 1, 3, 0, 1, 3, 3, 0, 1, 1, 0, 1, 0, 1, 1, 0, 2, 0, 0, 1, 1, 3, 3, 0, 1, 0, 0, 0, 2, 1, 2, 0, 0, 3, 0, 0, 1, 1, 0, 1, 1, 2, 1, 2, 0, 0, 0, 1, 0, 2, 0, 2, 1, 0, 1, 0, 0, 0, 2, 1, 3, 1, 0, 1, 1, 3, 0, 1, 2, 1, 3, 1, 0, 0, 1, 1, 1, 0, 0, 1, 0, 1, 3, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 2, 1, 3, 0, 0, 1, 0, 2, 0, 1, 0, 1, 1, 0, 0, 2, 3, 2, 0, 0, 0, 0, 0, 1, 1, 0, 0, 3, 1, 1, 2, 0, 0, 1, 3, 3, 3, 0, 3, 3, 1, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 0, 0, 0, 1, 2, 0, 3, 1, 0, 3, 0, 2, 0, 0, 2, 0, 3, 1, 0, 3, 3, 0, 0, 0, 0, 2, 3, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 1, 1, 3, 0, 0, 0, 2, 0, 0, 1, 2, 0, 2, 1, 0, 2, 1, 1, 0, 1, 0, 1, 0, 1, 0, 0, 2, 1, 1, 1, 3, 0, 1, 2, 1, 0, 1, 0, 0, 0, 0, 0, 1, 0, 0, 3, 0, 0, 1, 1, 0, 0, 0, 1, 0}

	// go func() {
	// 	i := 0
	// 	for {
	// 		skipList.Random <- oneSlice[i]
	// 		i++
	// 	}
	// }()

	funcVar := func(key string, currValue int, exists bool) (int, error) {
		if exists {
			//TODO fix
			return 3 + currValue, nil
		} else {
			return 3, nil
		}
	}

	myList := New[string, int]("myList   ", "", "\U0010FFFF")
	output, _ := myList.Upsert("1", funcVar)
	if output != 3 {
		t.Errorf("got output: %d", output)
	}

	val, ok := myList.Find("1")

	if !ok {
		t.Errorf("failed to find key 1")
	}

	if val != 3 {
		t.Errorf("key 1 had wrong value")
	}

	output, _ = myList.Upsert("2", funcVar)
	if output != 3 {
		t.Errorf("put failed")
	}
	_, ok = myList.Find("2")

	if !ok {
		t.Errorf("failed to find key 2")
	}

	output, _ = myList.Upsert("3", funcVar)
	if output != 3 {
		t.Errorf("put failed")
	}
	_, ok = myList.Find("3")

	if !ok {
		t.Errorf("failed to find key  3")
	}

	output, _ = myList.Upsert("4", funcVar)
	if output != 3 {
		t.Errorf("put failed")
	}
	_, ok = myList.Find("4")

	if !ok {
		t.Errorf("failed to find key 4")
	}

	output, _ = myList.Upsert("4", funcVar)
	if output != 3+3 {
		t.Errorf("put failed")
	}
	val, ok = myList.Find("4")

	if !ok {
		t.Errorf("failed to find key 4")
	}

	_, ok = myList.Find("2")

	if !ok {
		t.Errorf("failed to find key 2")
	}

	_, ok = myList.Find("1")

	if !ok {
		t.Errorf("failed to find key 1")
	}

	_, ok = myList.Remove("2")
	if !ok {
		t.Errorf("failed to remove key 2")
	}

	_, ok = myList.Remove("2")
	if ok {
		t.Errorf("Should have failed to remove 2")
	}

	_, ok = myList.Find("3")

	if !ok {
		t.Errorf("failed to find key 3")
	}

	_, ok = myList.Find("1")

	if !ok {
		t.Errorf("failed to find key 1")
	}

	_, ok = myList.Find("2")

	if ok {
		t.Errorf("should have failed to find key 2")
	}

	var wg sync.WaitGroup
	pointerList := New[string, *int]("myList", "", "\U0010FFFF")

	pointerFunc := func(key string, currValue *int, exists bool) (*int, error) {
		if exists {

			*currValue = (*currValue + 2)
			return currValue, nil
		} else {
			newInt := new(int)
			*newInt = 2
			return newInt, nil
		}
	}
	addIter := 100

	wg.Add(addIter)
	for iter := 0; iter < addIter; iter++ {

		go func() {
			defer wg.Done()

			_, err := pointerList.Upsert("test", pointerFunc)
			if err != nil {
				t.Errorf("got Error: %s", err.Error())
			}

		}()
	}
	wg.Wait()

	finalPointer, ok_findingTest := pointerList.Find("test")

	if !ok_findingTest || *finalPointer != 2*addIter {
		t.Errorf("failed to overwrite key 'test', got final value of %d instead of %d", *finalPointer, 2*addIter)
	}

	delIter := 1000
	wg.Add(delIter)

	for iter := 0; iter < delIter; iter++ {
		go func() {
			defer wg.Done()
			myList.Upsert(strconv.Itoa(iter), funcVar)
			//result, _ := myList.Upsert("test", funcVar)
			//num <- result
		}()

	}

	wg.Add(delIter)
	num := make(chan int)
	for key := 0; key < delIter; key++ {
		go func() {
			defer wg.Done()
			_, ok := myList.Remove(strconv.Itoa(key))
			if ok {
				num <- key
			} else {
				num <- -1
			}
		}()
	}

	var deletedKeys []string
	wg.Add(1)
	go func() {
		defer wg.Done()
		deleted := make([]string, 0)
		for key := 0; key < delIter; key++ {
			keyDeleted := <-num
			if keyDeleted != -1 {
				deleted = append(deleted, strconv.Itoa(keyDeleted))
			}
		}
		deletedKeys = deleted
	}()
	wg.Wait()

	copyFunc := func(num int) any {
		return num
	}
	queryKeys, queryValues, _ := myList.Query(context.TODO(), "", "\U0010FFFF", copyFunc)

	if len(queryValues)+len(deletedKeys) != delIter {
		t.Errorf("%d deleted and %d left", len(deletedKeys), len(queryValues))
	}

	for iter := 0; iter < len(queryKeys); iter++ {
		if slices.Contains(deletedKeys, queryKeys[iter]) {
			t.Errorf("%d deleted and %d left", len(deletedKeys), len(queryKeys))
		}
	}

	for iter := 0; iter < len(deletedKeys); iter++ {
		if slices.Contains(queryKeys, deletedKeys[iter]) {
			t.Errorf("%d deleted and %d left", len(deletedKeys), len(queryKeys))
		}
	}

}
