package db

import (
	"fmt"
	"testing"
)

func get(db *DB, k uint64, t uint64) {
	message, status, nextSequence, err := db.Get(Key(k), ValidTime(t))
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("key = %d, time = %d, message = %s, status = %s, nextSequence = %d\n", k, t, message, status, nextSequence)
}

func TestDB_Get(t *testing.T) {
	db := NewDB("", 0)
	for k := 1; k < 100; k++ {
		for j := 1; j < 100; j++ {
			if j%5 == 0 {
				continue
			}
			err := db.Put(Key(k), SequenceNumber(j), ValidTime(j*10), fmt.Sprintf("value %d", j))
			if err != nil {
				fmt.Println(err)
			}
		}
	}

	// Test the correctness of the Get function
	get(db, 0, 10)
	get(db, 1, 5)
	get(db, 1, 10)
	get(db, 1, 40)
	get(db, 1, 50)
	get(db, 1, 60)
	get(db, 1, 90)
	get(db, 1, 980)
	get(db, 1, 990)
	get(db, 1, 1000)

	err := db.Put(Key(100), SequenceNumber(1), ValidTime(10), fmt.Sprintf("value %d", 10))
	if err != nil {
		fmt.Println(err)
	}
	get(db, 100, 9)
	get(db, 100, 10)
	get(db, 100, 11)
}
