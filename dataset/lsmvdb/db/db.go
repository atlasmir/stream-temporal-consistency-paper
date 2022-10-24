package db

type DB struct {
	// file path
	path string

	// core data structures
	mem *Memtable

	// helper fields for experiments
	sensors map[int][]int // stores the sensor properties
}

func NewDB(path string, creationTime ValidTime) *DB {
	return &DB{
		path: path,
		mem:  NewMemtable(0),
	}
}

func (db *DB) Put(key Key, sequenceNumber SequenceNumber, creationTime ValidTime, value string) (err error) {
	// TODO: implement multiple components
	err = db.mem.Put(key, sequenceNumber, creationTime, value)
	return
}

func (db *DB) Get(key Key, time ValidTime) (message Message, status Status, nextSequence SequenceNumber, err error) {
	message, status, nextSequence, err = db.mem.Get(key, time)
	return
}

func (db *DB) SetSensors(sensors map[int][]int) {
	db.sensors = sensors
}
