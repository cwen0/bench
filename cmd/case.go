package cmd

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/cwen0/bench/utils"
	"github.com/urfave/cli"
)

var CmdPrepare = cli.Command{
	Name:   "prepare",
	Usage:  "Generateerate test case",
	Action: runPrepare,
	Flags: []cli.Flag{
		intFlag("count,c", 100, "case count"),
	},
}

func genRandomWriteCase(path string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Generate random write case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Create %s Error: %s", path, err)
	}
	tIndex := 0
	tSum := 0
	t := 1
	var sqlStr string
	for i := 0; i < count; i++ {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		tSum += (r.Intn(100) * r.Intn(50))
		index := 10000000000 + t*tSum
		sqlStr += fmt.Sprintf("indert into t (a, b, c) vakeus (%d, %f, %s);\n--\n", index, float64(index), "test")
		tIndex++
		t *= (-1)
		if tIndex >= 10 || i >= count-1 {
			_, err := file.WriteString(sqlStr)
			if err != nil {
				log.Fatalf("Write to %s Error: %s", path, err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
	file.Close()
}

func genRandomReadCase(path string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Generate random read case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Create %s Error: %s", path, err)
	}
	tIndex := 0
	var sqlStr string
	for i := 0; i < count; i++ {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		num := r.Intn(100000000)
		sqlStr += fmt.Sprintf("select a, b, c from t where a = %d;\n--\n", num)
		tIndex++
		if tIndex >= 10 || i >= count-1 {
			_, err := file.WriteString(sqlStr)
			if err != nil {
				log.Fatalf("Write to %s Error: %s", path, err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
	file.Close()
}

func genOrderWriteCase(path string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Generate oeder write case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Create %s Error: %s", path, err)
	}
	index := 1000000000
	tIndex := 0
	var sqlStr string
	for i := 0; i < count; i++ {
		index++
		sqlStr += fmt.Sprintf("indert into t (a, b, c) vakeus (%d, %f, %s);\n--\n", index, float64(index), "test")
		tIndex++
		if tIndex >= 10 || i >= count-1 {
			_, err := file.WriteString(sqlStr)
			if err != nil {
				log.Fatalf("Write to %s Error: %s", path, err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
	file.Close()
}

func genOrderReadCase(path string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Generate order read case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Create %s Error: %s", path, err)
	}
	tIndex := 0
	var sqlStr string
	for i := 0; i < count; i++ {
		sqlStr += fmt.Sprintf("select a, b, c from t where a = %d;\n--\n", i)
		tIndex++
		if tIndex >= 10 || i >= count-1 {
			_, err := file.WriteString(sqlStr)
			if err != nil {
				log.Fatalf("Write to %s Error: %s", path, err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
	file.Close()
}

func genRandomRWCase(path string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Generate random read and write case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := os.Create(path)
	if err != nil {
		log.Fatalf("Create %s Error: %s", path, err)
	}
	tIndex := 0
	tSum := 0
	flag := 1
	i := 0
	var sqlStr string
	writeToCseFile := func() {
		if tIndex >= 10 || i >= count-1 {
			_, err := file.WriteString(sqlStr)
			if err != nil {
				log.Fatalf("Write to %s Error: %s", path, err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
	for i < count {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		num := r.Intn(20)
		if flag > 0 {
			for j := 0; j < num; j++ {
				tSum += r.Intn(100 * num)
				index := 10000000000 + flag*tSum
				tIndex++
				sqlStr += fmt.Sprintf("indert into t (a, b, c) vakeus (%d, %f, %s);\n--\n", index, float64(index), "test")
				writeToCseFile()
				flag *= (-1)
				i++
				if i == count {
					break
				}
			}
		} else {
			for j := 0; j < num; j++ {
				tNum := r.Intn(100000000)
				sqlStr += fmt.Sprintf("select a, b, c from t where a = %d;\n--\n", tNum)
				writeToCseFile()
				flag *= (-1)
				i++
				if i == count {
					break
				}
			}
		}
	}
	file.Close()
}

func genCase(ctx *cli.Context) error {
	count := 100
	if ctx.IsSet("count") {
		count = ctx.Int("count")
	}
	curPath, err := os.Getwd()
	if err != nil {
		log.Fatalf("Get current path Error: %s", err)
	}
	casePath := curPath + "/case"
	err = utils.CreateDir(casePath)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(5)
	go genRandomWriteCase(casePath+"/random_write.case", count, &wg)
	go genRandomReadCase(casePath+"/random_read.case", count, &wg)
	go genOrderWriteCase(casePath+"/order_write.case", count, &wg)
	go genOrderReadCase(casePath+"/order_read.case", count, &wg)
	go genRandomRWCase(casePath+"/random_read_write.case", count, &wg)
	wg.Wait()
	return nil
}

func runPrepare(ctx *cli.Context) error {
	genCase(ctx)
	return nil
}
