package db

import "github.com/MauriceGit/skiplist"

type Memtable struct {
	creationTime ValidTime
	archiveTime  ValidTime
	// data is map of data streams indexed by sensor keys.
	// each data stream is also organized as a map indexed by the lower valid times of individual data versions.
	data map[Key]*skiplist.SkipList
}

func NewMemtable(creationTime ValidTime) *Memtable {
	data := make(map[Key]*skiplist.SkipList)
	return &Memtable{
		creationTime: creationTime,
		data:         data,
	}
}

// archiveMemtable TODO: implement the following functions
// minorCompact
func archiveMemtable(memtable *Memtable, archiveTime ValidTime) {
	memtable.archiveTime = archiveTime
}

func (mem Memtable) Put(key Key, sequenceNumber SequenceNumber, creationTime ValidTime, value string) (err error) {
	err = nil
	list, exist := mem.data[key]
	if exist != true {
		newVersionList := skiplist.New()
		mem.data[key] = &newVersionList
		list = mem.data[key]
	}
	list.Insert(Message{sequenceNumber: sequenceNumber, creationTime: creationTime, value: value})

	return
}

// Get function returns the message body, the status and the sequence number of the next message to a query.
// the returned status
func (mem Memtable) Get(key Key, time ValidTime) (message Message, status Status, nextSequence SequenceNumber, err error) {
	err = nil
	list, exist := mem.data[key]
	if exist != true {
		return Message{}, Status(NOTFOUND), 0, nil
	}
	elem, ok := list.FindGreaterOrEqual(Message{creationTime: time})
	if ok {
		message := elem.GetValue().(Message)
		var next Message
		if time == message.creationTime {
			// correct version
			if elem == list.GetLargestNode() {
				// this is the last version
				return message, Status(ODV), 0, nil
			}
			next = list.Next(elem).GetValue().(Message)
		} else {
			if elem == list.GetSmallestNode() {
				// this is the first version, and it has not been generated at time.
				return Message{}, Status(NOTFOUND), message.sequenceNumber, nil
			}
			// go backward
			next = message
			message = list.Prev(elem).GetValue().(Message)
		}
		nextSequence = next.sequenceNumber
		// Check the sequence number of the successive version
		if nextSequence == message.sequenceNumber+1 {
			// non-ODV. status set to OK
			return message, Status(OK), nextSequence, nil
		} else if nextSequence > message.sequenceNumber+1 {
			// HOLE
			return message, Status(HOLE), nextSequence, nil
		} else {
			// nextSequence <= message.sequenceNumber
			return Message{}, Status(ERROR), nextSequence, SequenceOutOfOrder{message.sequenceNumber, nextSequence}
		}
	} else {
		// matches last version
		return list.GetLargestNode().GetValue().(Message), Status(ODV), 0, nil
	}
}
