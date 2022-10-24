package db

import (
	"fmt"
	"gonum.org/v1/gonum/stat/distuv"
	"math"
	"time"
)

type Status int8
type Reason int8
type Key uint64
type SequenceNumber uint64
type ValidTime uint64

const (
	OK       Status = 0
	ODV             = 1
	HOLE            = 2
	NOTFOUND        = 3
	ERROR           = 4
)

const (
	NotCompleted  Reason = 0
	nonODV               = 1
	Timeout              = 2
	MaybeCorrect         = 3
	KeyNotInQuery        = 4
)

func (s Status) String() string {
	switch s {
	case OK:
		return "OK"
	case ODV:
		return "ODV"
	case HOLE:
		return "HOLE"
	case NOTFOUND:
		return "NOTFOUND"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// Message defines the message body stored in the memtable.
// it consists of the start valid time, the sequence number and the message value.
type Message struct {
	creationTime   ValidTime
	sequenceNumber SequenceNumber
	value          string
}

func NewMessage(creationTime ValidTime, sequenceNumber SequenceNumber, value string) *Message {
	return &Message{creationTime: creationTime, sequenceNumber: sequenceNumber, value: value}
}

func (m Message) CreationTime() ValidTime {
	return m.creationTime
}

func (m Message) SequenceNumber() SequenceNumber {
	return m.sequenceNumber
}

func (m Message) Value() string {
	return m.value
}

func (m Message) ExtractKey() float64 {
	return float64(m.creationTime)
}

func (m Message) String() string {
	return fmt.Sprintf("[vt = %09d, seq = %09d, value = %s]", m.creationTime, m.sequenceNumber, m.value)
}

/*
 * Query definitions
 */

type Result struct {
	message             *Message
	status              Status
	nextSequence        SequenceNumber
	probTemporalCorrect float64
}

type ResultWithInterval struct {
	message *Message
	start   ValidTime
	end     ValidTime
}

type Query struct {
	arrivalTime         ValidTime
	requestTime         ValidTime
	incomplete          int     // number of uncompleted keys
	probTemporalCorrect float64 // the probability that the query is temporally correct
	currentResults      map[Key]*Result
	pool                *QueryPool // the pool that the query belongs to
}

type QueryPool struct {
	size int
	pool map[Key]map[ValidTime]*Query

	// helper field for experiments
	sensors         map[int][]int // stores the sensor properties
	updateCount     int
	updateTotalTime int64
}

func (r *Result) Message() *Message {
	return r.message
}

func (r *ResultWithInterval) Message() *Message {
	return r.message
}

func (r *ResultWithInterval) Start() ValidTime {
	return r.start
}

func (r *ResultWithInterval) End() ValidTime {
	return r.end
}

func OverlappingInterval(results map[Key]ResultWithInterval) (lo, hi ValidTime, ok bool) {
	for _, result := range results {
		if result.Start() > lo {
			lo = result.Start()
		}
		if result.End() < hi {
			hi = result.End()
		}
	}
	return lo, hi, lo <= hi
}

func NewQuery(arrivalTime, requestTime ValidTime, incomplete int) *Query {
	return &Query{arrivalTime: arrivalTime, requestTime: requestTime, incomplete: incomplete, currentResults: make(map[Key]*Result)}
}

func (q *Query) SetPool(pool *QueryPool) {
	q.pool = pool
}

func (q *Query) ArrivalTime() ValidTime {
	return q.arrivalTime
}

func (q *Query) Result(key Key) *Result {
	return q.currentResults[key]
}

func (q *Query) CompleteOneKey() {
	q.incomplete--
}

func (q *Query) AllKeysOK() bool {
	return q.incomplete == 0
}

func (q *Query) NewResult(key Key, message *Message, status Status, nextSequence SequenceNumber, prob float64) {
	q.currentResults[key] = &Result{message: message, status: status, nextSequence: nextSequence, probTemporalCorrect: prob}
	q.updateProbTemporalCorrect()
}

func (q *Query) updateProbTemporalCorrect() {
	prob := 1.0
	for _, result := range q.currentResults {
		prob *= result.probTemporalCorrect
	}
	q.probTemporalCorrect = prob
}

func (q *Query) MaybeCorrect(ck float64) bool {
	return q.probTemporalCorrect >= ck
}

func (q *Query) Update(clock ValidTime, key Key, newMessage *Message, deadline ValidTime, ck float64) (completed, updated bool, finalReason Reason) {
	// check if the key is in the query
	currentResult, exist := q.currentResults[key]
	if exist != true {
		return false, false, KeyNotInQuery
	}
	// check if the query expires
	if clock > q.arrivalTime+deadline {
		return true, false, Timeout
	}

	// update the individual key requested in query
	keyCompleted, keyUpdated, reason := q.updateKey(key, currentResult, newMessage)
	if keyUpdated {
		q.updateProbTemporalCorrect()
		if q.probTemporalCorrect >= ck {
			return true, keyUpdated, MaybeCorrect
		}
	}
	if keyCompleted {
		q.incomplete--
		if q.AllKeysOK() {
			return true, keyUpdated, reason
		}
	}

	return false, keyUpdated, NotCompleted
}

// updateKey checks if a data stream requested by query q needs to be updated
// upon the arrival of a new message newMessage.
// It returns two bool values and a Reason:
//   - completed: whether the key is non ODV
//   - updated: whether the current result message is updated
//   - reason: the reason for the completion of the query on the data stream
func (q *Query) updateKey(key Key, currentResult *Result, newMessage *Message) (completed, updated bool, reason Reason) {
	currentMessage := currentResult.message
	if newMessage.CreationTime() > q.requestTime {
		// not found
		if currentResult.status == NOTFOUND {
			if newMessage.SequenceNumber() < currentResult.nextSequence || currentResult.nextSequence < 1 {
				currentResult.nextSequence = newMessage.SequenceNumber()
			}
			return false, false, NotCompleted
		}
		// hole or odv
		if newMessage.SequenceNumber() == currentMessage.SequenceNumber()+1 {
			// new message is the immediate successor of the current message, so we can confirm that the current message is not an ODV
			currentResult.nextSequence = newMessage.SequenceNumber()
			currentResult.status = OK
			return true, false, nonODV
		} else if newMessage.SequenceNumber() > currentMessage.SequenceNumber()+1 {
			if currentResult.status == ODV {
				currentResult.nextSequence = newMessage.SequenceNumber()
			} else {
				// current message is followed by a hole
				currentResult.nextSequence = SequenceNumber(math.Min(float64(currentResult.nextSequence),
					float64(newMessage.SequenceNumber())))
			}
			currentResult.status = HOLE
			return false, false, NotCompleted
		} else {
			// TODO: handle out of order sequence number
			panic(SequenceOutOfOrder{curr: newMessage.SequenceNumber(), next: currentMessage.SequenceNumber()})
		}
	} else {
		// newMessage.CreationTime() <= q.requestTime means the new message MAY be covered by the requested time range of the query
		if newMessage.SequenceNumber() < currentMessage.SequenceNumber() {
			return false, false, NotCompleted
		} else {
			// update entry because the new message is newer, and was valid at the requested time
			currentResult.message = newMessage
			// re-calculates the probability of temporal correctness
			mean := q.pool.sensors[int(key)][0]
			std := q.pool.sensors[int(key)][1]
			currentResult.probTemporalCorrect = ProbTemporalCorrect(mean, std, currentMessage.CreationTime(), q.requestTime)
			if currentResult.status == ODV {
				// new message is still ODV
				return false, true, NotCompleted
			} else {
				// current message is followed by a hole
				if newMessage.SequenceNumber() == currentResult.nextSequence-1 {
					currentResult.status = OK
					return true, true, nonODV
				} else {
					currentResult.status = HOLE
					return false, true, NotCompleted
				}
			}
		}
	}
}

func NewQueryPool() *QueryPool {
	return &QueryPool{pool: make(map[Key]map[ValidTime]*Query)}
}

func (qp *QueryPool) UpdateCount() int {
	return qp.updateCount
}

func (qp *QueryPool) UpdateTotalTime() int64 {
	return qp.updateTotalTime
}

func (qp *QueryPool) UpdateAverageTime() float64 {
	return float64(qp.updateTotalTime) / float64(qp.updateCount)
}

func (qp *QueryPool) Add(query *Query) {
	for key, _ := range query.currentResults {
		if _, exist := qp.pool[key]; exist != true {
			qp.pool[key] = make(map[ValidTime]*Query)
		}
		qp.pool[key][query.arrivalTime] = query
	}
	qp.size++
}

func (qp *QueryPool) SetSensors(sensors map[int][]int) {
	qp.sensors = sensors
}

// Update scans the query pool upon the arrival of a new message newMessage to update corresponding queries.
// It returns a list of completed queries and a list of updated queries.
//   - completedQueries: a slice of completed queries which are removed from the query pool
//   - updatedQueries: a slice of queries. In such a query, at least one of its current data stream is updated with newMessage.
//
// Note that if a query is completed, it will NOT appear in the updatedQueries list.
func (qp *QueryPool) Update(clock ValidTime, key Key, newMessage *Message, deadline ValidTime, ck float64) (completedQueries, updatedQueries []*Query) {
	// update the query pool
	queries, exist := qp.pool[key]
	if exist != true {
		return
	}
	// update each query
	completedQueries = make([]*Query, 0)
	updatedQueries = make([]*Query, 0)
	for arrivalTime, query := range queries {
		// TODO: handle query completion reasons
		startTime := time.Now()
		completed, updated, _ := query.Update(clock, key, newMessage, deadline, ck)
		qp.updateTotalTime += time.Since(startTime).Microseconds()
		qp.updateCount++
		if completed {
			completedQueries = append(completedQueries, query)
			// remove the reference to the query from each key
			for key, _ := range query.currentResults {
				delete(qp.pool[key], arrivalTime)
			}
			qp.size--
		} else if updated {
			updatedQueries = append(updatedQueries, query)
		}
	}
	return // completedQueries
}

/*
 * The probability of a message to be temporal correct.
 */

func ProbTemporalCorrect(mean, stddev int, creationTime, requestTime ValidTime) float64 {
	dist := distuv.Normal{
		Mu:    float64(mean),
		Sigma: float64(stddev),
	}
	return 1.0 - dist.CDF(float64(requestTime-creationTime))
}

/*
 * Error definitions
 */

// SequenceOutOfOrder defines an error where the sequence number next <= curr
type SequenceOutOfOrder struct {
	curr SequenceNumber
	next SequenceNumber
}

func (o SequenceOutOfOrder) Error() string {
	return fmt.Sprintf("Error: Sequences are out of order. curr = %d, next = %d", o.curr, o.next)
}
