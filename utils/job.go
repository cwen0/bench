package utils

import (
	"database/sql"
	"fmt"
	"time"
)

func AddJobs(count int, jobChan chan struct{}) {
	for i := 0; i < count; i++ {
		jobChan <- struct{}{}
	}
	close(jobChan)
}

func Waiting(doneChan chan struct{}, start time.Time, jobCount int, workerCount int) {
	for i := 0; i < workerCount; i++ {
		<-doneChan
	}
	close(doneChan)
	now := time.Now()
	seconds := now.Unix() - start.Unix()
	tps := int64(-1)
	if seconds > 0 {
		tps = int64(jobCount) / seconds
	}
	fmt.Printf("total %d cases, cost %d seconds, tps %d, start %s, now %s\n", jobCount, seconds, tps, start, now)
}

func HandleJob(db *sql.DB, testData []string, batch int, jobChan chan struct{}, doneChan chan struct{}) {
}
