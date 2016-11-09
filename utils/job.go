package utils

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/cwen0/bench/lib/resp"
)

func Waiting(doneChan chan struct{}, resChan chan resp.RespTime, start time.Time, jobCount int, workerCount int, commitC int) {
	for i := 0; i < workerCount; i++ {
		<-doneChan
	}
	close(doneChan)
	now := time.Now()
	seconds := now.Unix() - start.Unix()
	qps := int64(-1)
	if seconds > 0 {
		qps = int64(jobCount) / seconds
	}
	fmt.Printf("total %d cases, cost %d seconds, qps %d, tps %d, start %s, now %s\n", jobCount, seconds, qps, qps/int64(commitC), start, now)
	var avgSum int64
	tRes := <-resChan
	tRes.Count()
	avgSum = tRes.AvgTime
	min := tRes.MinTime
	max := tRes.MaxTime
	for i := 1; i < workerCount; i++ {
		res := <-resChan
		res.Count()
		avgSum += res.AvgTime
		if max < res.MaxTime {
			max = res.MaxTime
		}
		if min > res.MinTime {
			min = res.MinTime
		}
	}
	fmt.Println("--------response time-------- ")
	fmt.Printf("avg     %f ms\n", float64(avgSum/int64(workerCount))/1000000)
	fmt.Printf("min     %f ms\n", float64(min)/1000000)
	fmt.Printf("max     %f ms\n", float64(max)/1000000)
}

func HandleJob(db *sql.DB, data []string, batch int, resChan chan resp.RespTime, doneChan chan struct{}) {
	var res resp.RespTime
	if batch <= 0 {
		doExec(db, data, &res)
	} else {
		temp := 0
		count := 0
		lenData := len(data)
		for count < lenData {
			temp++
			if temp == batch {
				doTranscationExec(db, data[count-batch+1:count+1], &res)
				temp = 0
			}
			count++
		}

		if temp > 0 {
			temp = 0
			doTranscationExec(db, data[count-1:lenData], &res)
		}
	}
	doneChan <- struct{}{}
	resChan <- res
}

func doExec(db *sql.DB, data []string, res *resp.RespTime) {
	for _, sql := range data {
		if sql == "" {
			continue
		}
		start := time.Now()
		_, err := db.Exec(sql)
		if err != nil {
			log.Fatalf("Exec sql Error: %s", err)
		}
		res.TimesArr = append(res.TimesArr, time.Since(start))
	}
}

func doTranscationExec(db *sql.DB, data []string, res *resp.RespTime) {
	txn, err := db.Begin()
	if err != nil {
		log.Fatalf("Transaction bengin Error: %s", err)
	}
	for _, sql := range data {
		if sql == "" {
			continue
		}
		start := time.Now()
		_, err := txn.Exec(sql)
		if err != nil {
			log.Fatalf("Exec sql Error: %s", err)
		}
		res.TimesArr = append(res.TimesArr, time.Since(start))
	}
	err = txn.Commit()
	if err != nil {
		log.Fatalf("Transcation commit Error: %s", err)
	}
}
