package dataset

import (
	"encoding/csv"
	"fmt"
	"github.com/atlasmir/golsmvdb/lsmvdb/db"
	"log"
	"os"
	"testing"
)

func TestGenerateOfflineInstructions(t *testing.T) {
	generateOfflineInstructions(3000, 3000, 1)
}

func TestReadSensorProperties(t *testing.T) {
	sensors := readSensorProperties("input/sensors.json")
	fmt.Println(sensors)
}

func TestExecuteInstructions(t *testing.T) {
	stats := executeInstructions("input/instructions.txt", 2000, 0.0, readSensorProperties("input/sensors.json"))

	fmt.Printf("Inconsistent Results = %f\n", stats["inconsistent_results"])
	fmt.Printf("Total number of queries: %f\n", stats["total_queries"])
	fmt.Printf("Total number of query pool scans: %f\n", stats["scan_count"])
	fmt.Printf("Average time spent scanning query pool: %f micros\n",
		stats["time_scan_query_pool"]/stats["scan_count"])
	fmt.Printf("Average time spent for query first exexution: %f micros\n", stats["time_first_execution"]/stats["total_queries"])
}

func TestBackwardExecutionOption(t *testing.T) {
	file, err := os.OpenFile("output/beo.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("failed to open file:", err)
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			log.Fatalln(err)
		}
	}(file)

	header := []string{"dkBackward", "dkForward", "numberOfQueries", "beoSuccess", "nonSuccess", "timeouts", "responseTime"}
	write := csv.NewWriter(file)
	if err := write.Write(header); err != nil {
		log.Fatalln("error writing header to csv:", err)
	}
	defer write.Flush()

	for _, dkForward := range []int{2000, 3000, 4000, 5000, 6000} {
		for _, dk := range []int{2000, 2500, 3000, 3500, 4000, 4500, 5000} {
			generateOfflineInstructions(3000, dkForward, 10)
			stats := executeInstructions("input/instructions.txt", db.ValidTime(dk), 1.0, readSensorProperties("input/sensors.json"))
			record := []string{
				fmt.Sprintf("%d", dkForward-1000), // assume mq = 1000
				fmt.Sprintf("%d", dk),
				fmt.Sprintf("%f", stats["total_queries"]),
				fmt.Sprintf("%f", stats["ok_count"]),
				fmt.Sprintf("%f", stats["inconsistent_results"]),
				fmt.Sprintf("%f", stats["timeouts"]),
				fmt.Sprintf("%f", stats["total_response_time"]/stats["total_queries"]),
			}
			if err := write.Write(record); err != nil {
				log.Fatalln("error writing record to csv:", err)
			}
		}
	}

}

func BenchmarkQpESOverhead(b *testing.B) {
	file, err := os.OpenFile("output/qpES_overhead.txt", os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalln("failed to open file:", err)
	}
	defer func(file *os.File) {
		if err := file.Close(); err != nil {
			log.Fatalln(err)
		}
	}(file)

	header := []string{"keysPerQuery", "dk", "ck", "numberOfQueries", "firstExecutionTime", "scanTime", "scanCount", "averageUpdateTime"}
	writer := csv.NewWriter(file)
	if err := writer.Write(header); err != nil {
		log.Fatalln("error writing header to csv:", err)
	}
	defer writer.Flush()

	for _, k := range []int{1, 5, 10} {
		generateOfflineInstructions(3000, 3000, k)
		for d := 2000; d < 5001; d += 500 {
			for c := 0.2; c < 1.1; c += 0.4 {
				stats := executeInstructions("input/instructions.txt", db.ValidTime(d), c, readSensorProperties("input/sensors.json"))
				record := []string{
					fmt.Sprintf("%d", k),
					fmt.Sprintf("%d", d),
					fmt.Sprintf("%f", c),
					fmt.Sprintf("%f", stats["total_queries"]),
					fmt.Sprintf("%f", stats["time_first_execution"]/stats["total_queries"]),
					fmt.Sprintf("%f", stats["time_scan_query_pool"]/stats["scan_count"]),
					fmt.Sprintf("%f", stats["scan_count"]),
					fmt.Sprintf("%f", stats["update_average_time"]),
				}
				if err := writer.Write(record); err != nil {
					log.Fatalln("error writing record to csv:", err)
				}

			}
		}
	}
}
