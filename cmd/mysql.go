package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/cwen0/bench/utils"
	_ "github.com/go-sql-driver/mysql"
	"github.com/urfave/cli"
)

var CmdMysql = cli.Command{
	Name:   "mysql",
	Usage:  "mysql bench test",
	Action: runMysql,
	Flags: []cli.Flag{
		stringFlag("username,u", "root", "mysql username"),
		stringFlag("password,p", "", "mysql password"),
		stringFlag("host", "127.0.0.1", "mysql host addr"),
		stringFlag("port,P", "3306", "mysql listen port"),
		stringFlag("database,d", "test", "test database"),
		stringFlag("case-path", "", "test case path"),
		intFlag("worker-count", 1, "parallel worker count"),
		intFlag("commit-count", 1, "batch commit count"),
		boolFlag("clean", "clean test table"),
	},
}

type mysql struct {
	user             string
	password         string
	host             string
	port             string
	dbName           string
	casePath         string
	workerCount      int
	batchCommitCount int
	isClean          bool
	testData         []string
	db               *sql.DB
}

func newMysql(ctx *cli.Context) *mysql {
	if !ctx.IsSet("case-path") {
		log.Fatal("Argument case-path is must")
	}
	return &mysql{
		user:             ctx.String("username"),
		password:         ctx.String("password"),
		host:             ctx.String("host"),
		port:             ctx.String("port"),
		dbName:           ctx.String("database"),
		casePath:         ctx.String("case-path"),
		workerCount:      ctx.Int("worker-count"),
		batchCommitCount: ctx.Int("commit-count"),
		isClean:          ctx.Bool("clean"),
	}
}

func (m *mysql) readTestData() {
	if m.casePath == "" {
		log.Fatal("case-path can not empty")
	}
	data, err := ioutil.ReadFile(m.casePath)
	if err != nil {
		log.Fatalf("ReadFile Error: %s", err)
	}
	if len(data) == 0 {
		log.Fatal("Case file is empty")
	}
	dataArr := bytes.Split(data, []byte("\n--"))
	for _, v := range dataArr {
		str := strings.TrimSpace(strings.Trim(string(v), "\n"))
		str = strings.Trim(str, "\n")
		if str == "" {
			continue
		}
		m.testData = append(m.testData, str)
	}
}

func (m *mysql) openDB() {
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", m.user, m.password, m.host, m.port, m.dbName)
	dbTemp, err := sql.Open("mysql", dbAddr)
	if err != nil {
		log.Fatalf("Open mysql db Error: %s", err)
	}
	m.db = dbTemp
}

func (m *mysql) test() {
	count := len(m.testData)
	var doneChan chan struct{}
	if count < m.workerCount {
		doneChan = make(chan struct{}, count)
	} else {
		doneChan = make(chan struct{}, m.workerCount)
	}
	start := time.Now()
	if count < m.workerCount {
		for i := 0; i < count; i++ {
			go utils.HandleJob(m.db, m.testData[i:i+1], m.batchCommitCount, doneChan)
		}
	} else {
		num := count / m.workerCount
		for i := 0; i < m.workerCount; i++ {
			if i == m.workerCount-1 {
				go utils.HandleJob(m.db, m.testData[i*num:count], m.batchCommitCount, doneChan)
				break
			}
			go utils.HandleJob(m.db, m.testData[i*num:(i+1)*num], m.batchCommitCount, doneChan)
		}
	}
	if count < m.workerCount {
		utils.Waiting(doneChan, start, count, count)
	} else {
		utils.Waiting(doneChan, start, count, m.workerCount)
	}
}

func (m *mysql) clean() {
	_, err := m.db.Query("drop table t")
	if err != nil {
		log.Fatalf("Clean table Error: %s", err)
	}
}

func runMysql(ctx *cli.Context) error {
	mql := newMysql(ctx)
	mql.readTestData()
	mql.openDB()
	mql.test()
	if mql.isClean {
		mql.clean()
	}
	return nil
}
