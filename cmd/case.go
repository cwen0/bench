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
	Usage:  "Generate test case",
	Action: runPrepare,
	Flags: []cli.Flag{
		intFlag("count,c", 100, "case count"),
	},
}

func genRandomWriteCase(dir string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Gen random write case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := utils.CreateFile(dir, "random_write.case")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
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
				log.Fatalf("Write to %s Error: %s", dir+"/random_write.case", err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
}

func genRandomReadCase(dir string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Gen random read case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := utils.CreateFile(dir, "random_read.case")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	tIndex := 0
	var sqlStr string
	for i := 0; i < count; i++ {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		num := r.Intn(100000000)
		sqlStr += fmt.Sprintf("select (a, b, c) from t where a = %d;\n--\n", num)
		tIndex++
		if tIndex >= 10 || i >= count-1 {
			_, err := file.WriteString(sqlStr)
			if err != nil {
				log.Fatalf("Write to %s Error: %s", dir+"/random_read.case", err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
}

func genOrderWriteCase(dir string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Gen oeder write case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := utils.CreateFile(dir, "order_write.case")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
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
				log.Fatalf("Write to %s Error: %s", dir+"/order_write.case", err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
}

func genOrderReadCase(dir string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Gen order read case cast: %s", elapsed)
		wg.Done()
	}()
	file, err := utils.CreateFile(dir, "order_read.case")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
	}
	tIndex := 0
	var sqlStr string
	for i := 0; i < count; i++ {
		sqlStr += fmt.Sprintf("select (a, b, c) from t where a = %d;\n--\n", i)
		tIndex++
		if tIndex >= 10 || i >= count-1 {
			_, err := file.WriteString(sqlStr)
			if err != nil {
				log.Fatalf("Write to %s Error: %s", dir+"/order_read.case", err)
			}
			tIndex = 0
			sqlStr = ""
		}
	}
}

func genRandomRWCase(dir string, count int, wg *sync.WaitGroup) {
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Gen random read and write case cast: %s", elapsed)
		wg.Done()
	}()

	file, err := utils.CreateFile(dir, "random_read_write.case")
	defer file.Close()
	if err != nil {
		log.Fatal(err)
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
				log.Fatalf("Write to %s Error: %s", dir+"/random_read_write.case", err)
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
				sqlStr += fmt.Sprintf("select (a, b, c) from t where a = %d;\n--\n", tNum)
				writeToCseFile()
				flag *= (-1)
				i++
				if i == count {
					break
				}
			}
		}
	}
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
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(5)
	go genRandomWriteCase(casePath, count, &wg)
	go genRandomReadCase(casePath, count, &wg)
	go genOrderWriteCase(casePath, count, &wg)
	go genOrderReadCase(casePath, count, &wg)
	go genRandomRWCase(casePath, count, &wg)
	wg.Wait()
	return nil
}

func runPrepare(ctx *cli.Context) error {
	genCase(ctx)
	return nil
}
