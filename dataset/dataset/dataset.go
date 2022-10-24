package dataset

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/atlasmir/golsmvdb/lsmvdb/db"
	"golang.org/x/exp/slices"
	"io"
	"log"
	"math"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const k = 1000
const clockMax = 1_000_000

type Operation uint8

const (
	Get Operation = iota
	Put
)

// queryResult is a helper struct used to compare the results between different runs.
type queryResult struct {
	result map[db.Key]db.SequenceNumber
}

func newQueryResult() *queryResult {
	return &queryResult{
		result: make(map[db.Key]db.SequenceNumber),
	}
}

func (r *queryResult) update(key db.Key, seq db.SequenceNumber) {
	r.result[key] = seq
}

func equal(a, b *queryResult) bool {
	if len(a.result) != len(b.result) {
		return false
	}
	for i := range a.result {
		if a.result[i] != b.result[i] {
			return false
		}
	}
	return true
}

// Atoi is a wrapper of strconv.Atoi(s string) which panics if the conversion fails.
func Atoi(s string) int {
	i, err := strconv.Atoi(s)
	if err != nil {
		log.Fatalln("failed to convert string to int", err)
	}
	return i
}

// randomInt returns a random integer in [min, max)
func randomInt(min, max int) int {
	return min + rand.Intn(max-min)
}

func normFloat64(mean, stdDev float64) float64 {
	return rand.NormFloat64()*stdDev + mean
}

func normIntPositive(mean, stdDev int) int {
	return int(math.Max(1, normFloat64(float64(mean), float64(stdDev))))
}

func convertIntArrayToStringArray(arr []int) []string {
	strArr := make([]string, len(arr))
	for i, v := range arr {
		strArr[i] = strconv.Itoa(v)
	}
	return strArr
}

func assembleRow(arr []int) []string {
	var op string
	if arr[3] == 0 {
		op = "0"
	} else {
		op = "1"
	}
	return slices.Insert(convertIntArrayToStringArray(arr), 1, op)
}

func mergeTwoSortedSlices(a, b [][]int) [][]int {
	i, j := 0, 0
	result := make([][]int, 0)
	for i < len(a) && j < len(b) {
		if a[i][0] < b[j][0] {
			result = append(result, a[i])
			i++
		} else {
			result = append(result, b[j])
			j++
		}
	}
	if i < len(a) {
		result = append(result, a[i:]...)
	}
	if j < len(b) {
		result = append(result, b[j:]...)
	}
	return result
}

// mergeKSortedSlices merges k sorted slices into one sorted slice.
// The input arr is a slice of slices, whose length is k.
// Each slice arr[i] is a 2-D slice (i.e., var arr[i] [][]int).
// The order of two elements is determined by the first attribute,
// i.e., the order of arr[i][j] and arr[i][k] is determined by arr[i][j][0] and arr[i][k][0].
func mergeKSortedSlices(arr [][][]int) [][]int {
	if len(arr) == 1 {
		return arr[0]
	}
	return mergeTwoSortedSlices(mergeKSortedSlices(arr[:len(arr)/2]), mergeKSortedSlices(arr[len(arr)/2:]))
}

func generateOfflineInstructions(meanDataTransmission, meanQueryTransmission, keysPerQuery int) {
	if keysPerQuery < 1 {
		log.Fatalln("keysPerQuery is at least 1")
	}
	// Randomize the seed
	rand.Seed(time.Now().UnixNano())

	/*
	 * Generate the sensor properties
	 */
	sensors := make(map[int][]int)
	for i := 0; i < k; i++ {
		sensors[i] = []int{randomInt(500, 1500), randomInt(300, 500)}
	}
	jsonStr, err := json.Marshal(sensors)
	if err != nil {
		log.Fatalln("failed to convert sensors map into json string", err)
	} else {
		writeFile("input/sensors.json", jsonStr)
	}

	/*
	 * Generate data streams
	 */
	streams := make([][][]int, k+1) // +1 for the query streams
	for i := 0; i < k; i++ {
		streams[i] = make([][]int, 0)
		clock := 0
		sequenceNumber := 0
		for clock < clockMax {
			generationTime := clock + normIntPositive(sensors[i][0], sensors[i][1])
			clock = generationTime
			arrivalTime := generationTime + normIntPositive(meanDataTransmission, 1000)
			sequenceNumber++
			streams[i] = append(streams[i], []int{arrivalTime, i, generationTime, sequenceNumber})
		}
		slices.SortFunc(streams[i], func(i, j []int) bool { return i[0] < j[0] })
	}

	/*
	 * Generate queries
	 * and let streams[k] points to queries
	 */
	queries := make([][]int, 0)
	clock := 10_000
	for clock < clockMax {
		queryArrivalTime := clock + randomInt(15, 25)
		clock = queryArrivalTime
		queryTargetTime := queryArrivalTime - normIntPositive(meanQueryTransmission, 1000)
		key := randomInt(0, k)
		queryRow := []int{queryArrivalTime, key, queryTargetTime, 0}
		if keysPerQuery > 1 {
			for i := 1; i < keysPerQuery; i++ {
				key = randomInt(0, k)
				queryRow = append(queryRow, key)
			}
		}
		queries = append(queries, queryRow)

	}
	slices.SortFunc(queries, func(i, j []int) bool { return i[0] < j[0] })
	streams[k] = queries

	// build header
	header := []string{"arr", "op", "key", "time", "seq"}

	data := mergeKSortedSlices(streams)
	writeCSVFile("input/instructions.txt", header, data)
}

func writeFile(filename string, data []byte) {
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatalln("failed to close file", err)
		}
	}(f)

	_, err = f.Write(data)
	if err != nil {
		log.Fatalln("failed to write file", err)
	}
}

func writeCSVFile(filename string, header []string, data [][]int) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln("failed to close file", err)
		}
	}(file)

	// write the csv file
	writer := csv.NewWriter(file)
	defer writer.Flush()

	if err := writer.Write(header); err != nil {
		log.Fatalln("error writing header to file", err)
	}
	for _, row := range data {
		if err := writer.Write(assembleRow(row)); err != nil {
			log.Fatalln("error writing record to file", err)
		}
	}
}

/*
 * Execute the instructions and collect statistics.
 */

func readSensorProperties(filename string) map[int][]int {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln("failed to close file", err)
		}
	}(file)

	byteValue, err := io.ReadAll(file)
	if err != nil {
		log.Fatalln("failed to read file", err)
	}

	var sensors map[int][]int
	err = json.Unmarshal(byteValue, &sensors)
	if err != nil {
		log.Fatalln("failed to unmarshal json", err)
	}

	return sensors
}

// instruction defines the parsed instruction format.
// for Put operation, validTime is the generation time of the incoming data version.
// for Get operation, validTime is the target time of the query.
type instruction struct {
	op             Operation
	key            db.Key
	validTime      db.ValidTime
	sequenceNumber db.SequenceNumber
	numberOfKeys   int
	additionalKeys []db.Key
}

func parseInstruction(row []string, clock *db.ValidTime) (inst instruction) {
	*clock = db.ValidTime(Atoi(row[0]))
	inst.op = Operation(Atoi(row[1]))
	inst.key = db.Key(Atoi(row[2]))
	inst.validTime = db.ValidTime(Atoi(row[3]))
	inst.sequenceNumber = db.SequenceNumber(Atoi(row[4]))
	inst.numberOfKeys = len(row) - 4

	if inst.op == Get && inst.numberOfKeys > 1 {
		inst.additionalKeys = make([]db.Key, inst.numberOfKeys-1)
		for i := 0; i < inst.numberOfKeys-1; i++ {
			inst.additionalKeys[i] = db.Key(Atoi(row[5+i]))
		}
	}
	return
}

// executeInstructions reads the instructions from the file and apply the instructions to a sample database.
// It scans the instruction file for two times:
//   - the first round: executes all the Put() and Get() instructions following the arrival time order.
//   - the second round: executes all the Get() instructions based on the already-filled database. It is assumed that
//     there is no temporal incorrect query results in this round.
//
// It returns the statistics of the execution.
func executeInstructions(filename string, deadline db.ValidTime, correctness float64, sensors map[int][]int) (stats map[string]float64) {
	//sensors := readSensorProperties("input/sensors.json")
	file, err := os.Open(filename) // open as read-only
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalln("failed to close file", err)
		}
	}(file)

	// Initialize the database
	clock := db.ValidTime(0)
	sampleDB := db.NewDB("", db.ValidTime(clock))
	queryPool := db.NewQueryPool()
	queryPool.SetSensors(sensors)
	fmt.Printf("Deadline = %d\n", deadline)
	fmt.Printf("Correctness Threshold = %f\n", correctness)

	// Statistics
	results1 := make(map[db.ValidTime]*queryResult, 0)
	stats = map[string]float64{
		"total_queries":        0,
		"total_response_time":  0,
		"removed_queries":      0,
		"odv_count":            0,
		"recheck_count":        0,
		"ok_count":             0, // number of queries that immediately completes with non-ODV result
		"ck_count":             0, // number of queries that completes because of satisfying the correctness threshold
		"timeout_count":        0, // number of queries that completes because of timeout
		"time_scan_query_pool": 0, // real time cost of scanning the query pool
		"scan_count":           0, // number of times the query pool is scanned
		"time_first_execution": 0,
		"inconsistent_results": 0,
		"update_average_time":  0, // the average time cost of updating a query in the query pool
	}

	// Read the csv file
	reader1 := csv.NewReader(file)
	reader1.FieldsPerRecord = -1 // allow variable number of keys per query
	_, err = reader1.Read()      // skip the header
	if err != nil {
		log.Fatalln("error reading header from file", err)
	}
	for {
		row, err := reader1.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalln("error reading record from csv file", err)
		}
		//clock, _ = strconv.Atoi(row[0])
		//operation, _ := strconv.Atoi(row[1])
		//key, _ := strconv.Atoi(row[2])
		//validTime, _ := strconv.Atoi(row[3])
		//seq, _ := strconv.Atoi(row[4])
		inst := parseInstruction(row, &clock)

		switch inst.op {
		case Get: // query
			stats["total_queries"]++
			requestedKeys := make([]db.Key, inst.numberOfKeys)
			requestedKeys[0] = inst.key // the first requested key in the query
			query := db.NewQuery(clock, inst.validTime, inst.numberOfKeys)
			results1[clock] = newQueryResult()
			for i, k := range inst.additionalKeys {
				requestedKeys[i+1] = k
			}
			// iterate over the keys in a single query
			timeStart := time.Now()
			for _, k := range requestedKeys {
				message, status, nextSequence, err := sampleDB.Get(k, inst.validTime)
				if err != nil {
					log.Fatalln("failed to get value from memtable", err)
				}
				prob := db.ProbTemporalCorrect(sensors[int(k)][0], sensors[int(k)][1], message.CreationTime(), inst.validTime)
				query.NewResult(k, &message, status, nextSequence, prob)
				if status == db.OK {
					query.CompleteOneKey()
				}
				// append result for experiment statistics
				results1[clock].update(k, message.SequenceNumber())
			}
			stats["time_first_execution"] += float64(time.Since(timeStart).Microseconds())

			if !query.AllKeysOK() && !query.MaybeCorrect(correctness) {
				// query is not immediately satisfied. Add it to the query pool.
				queryPool.Add(query)
				query.SetPool(queryPool)
			} else if query.AllKeysOK() {
				// query is immediately satisfied
				stats["ok_count"]++
			}

		case Put: // insert
			value := fmt.Sprintf("value: %d", inst.sequenceNumber)
			err = sampleDB.Put(inst.key, inst.sequenceNumber, inst.validTime, value)
			if err != nil {
				log.Fatalln("failed to insert", err)
			}
			timeStart := time.Now()
			completedQueries, updatedQueries := queryPool.Update(
				clock, inst.key,
				db.NewMessage(inst.validTime, inst.sequenceNumber, value),
				deadline, correctness)
			stats["time_scan_query_pool"] += float64(time.Since(timeStart).Microseconds())
			stats["scan_count"]++
			for _, q := range completedQueries {
				responseTime := float64(clock - q.ArrivalTime())
				if responseTime > float64(deadline) {
					stats["timeouts"]++
					stats["total_response_time"] += float64(deadline)
				} else {
					stats["total_response_time"] += responseTime
				}

				result := results1[q.ArrivalTime()]
				for k := range result.result {
					result.update(k, q.Result(k).Message().SequenceNumber())
				}
			}
			for _, q := range updatedQueries {
				result := results1[q.ArrivalTime()]
				for k := range result.result {
					result.update(k, q.Result(k).Message().SequenceNumber())
				}
			}
		default:
			log.Fatalln("unknown operation", inst.op)
		}
	}
	stats["update_average_time"] = queryPool.UpdateAverageTime()

	/*
	 * Second round of execution
	 */
	results2 := make(map[db.ValidTime]*queryResult)

	_, err = file.Seek(0, io.SeekStart) // rewind the file
	if err != nil {
		log.Fatalln("failed to rewind file", err)
	}
	reader2 := csv.NewReader(file)
	reader2.FieldsPerRecord = -1
	_, err = reader2.Read() // skip the header
	if err != nil {
		log.Fatalln("error reading header from file", err)
	}
	for {
		row, err := reader2.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalln("error reading record from csv file", err)
		}
		clock = db.ValidTime(Atoi(row[0]))
		operation, _ := strconv.Atoi(row[1])
		key, _ := strconv.Atoi(row[2])
		validTime, _ := strconv.Atoi(row[3])
		//seq, _ := strconv.Atoi(row[4])

		switch operation {
		case 0: // query
			numberOfKeys := len(row) - 4
			requestedKeys := make([]int, numberOfKeys)
			requestedKeys[0] = key
			results2[db.ValidTime(clock)] = newQueryResult()
			for i := 5; i < len(row); i++ {
				k, _ := strconv.Atoi(row[i])
				requestedKeys[i-4] = k
			}
			// iterate over the keys in a single query
			for _, k := range requestedKeys {
				message, _, _, err := sampleDB.Get(db.Key(k), db.ValidTime(validTime))
				if err != nil {
					log.Fatalln("failed to get value from memtable", err)
				}
				// append result
				results2[db.ValidTime(clock)].update(db.Key(k), message.SequenceNumber())
			}
		case 1: // insert
			// do nothing
		default:
			log.Fatalln("unknown operation", operation)
		}
	}

	// Compare the results
	for queryArrivalTime, result1 := range results1 {
		result2, ok := results2[queryArrivalTime]
		if !ok {
			log.Fatalln("corresponding record not found in result2", queryArrivalTime)
		}
		if !equal(result1, result2) {
			stats["inconsistent_results"]++
		}
	}

	fmt.Printf("Size of the first result set: %d\n", len(results1))
	fmt.Printf("Size of the second result set: %d\n", len(results2))
	return
}
