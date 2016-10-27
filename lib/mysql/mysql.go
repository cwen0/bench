package mysql

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"github.com/cwen0/bench/lib/resp"
	"github.com/cwen0/bench/utils"
	"github.com/urfave/cli"
)

type mysql struct {
	user             string
	password         string
	host             string
	port             string
	dbName           string
	casePath         string
	workerCount      int
	batchCommitCount int
	isTranscation    bool
	isClean          bool
	testData         []string
	db               *sql.DB
}

func NewMysql(ctx *cli.Context) *mysql {
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
		isTranscation:    ctx.Bool("transcation"),
		isClean:          ctx.Bool("clean"),
	}
}

func (m *mysql) ReadTestData() {
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

func (m *mysql) OpenDB() {
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8", m.user, m.password, m.host, m.port, m.dbName)
	dbTemp, err := sql.Open("mysql", dbAddr)
	if err != nil {
		log.Fatalf("Open mysql db Error: %s", err)
	}
	m.db = dbTemp
}

func (m *mysql) Test() {
	count := len(m.testData)
	var doneChan chan struct{}
	var responnseChan chan resp.RespTime
	if count < m.workerCount {
		doneChan = make(chan struct{}, count)
		responnseChan = make(chan resp.RespTime, count)
	} else {
		doneChan = make(chan struct{}, m.workerCount)
		responnseChan = make(chan resp.RespTime, m.workerCount)
	}
	if !m.isTranscation {
		m.batchCommitCount = -1
	}
	start := time.Now()
	if count < m.workerCount {
		for i := 0; i < count; i++ {
			go utils.HandleJob(m.db, m.testData[i:i+1], m.batchCommitCount, responnseChan, doneChan)
		}
	} else {
		num := count / m.workerCount
		for i := 0; i < m.workerCount; i++ {
			if i == m.workerCount-1 {
				go utils.HandleJob(m.db, m.testData[i*num:count], m.batchCommitCount, responnseChan, doneChan)
				break
			}
			go utils.HandleJob(m.db, m.testData[i*num:(i+1)*num], m.batchCommitCount, responnseChan, doneChan)
		}
	}
	if count < m.workerCount {
		utils.Waiting(doneChan, responnseChan, start, count, count)
	} else {
		utils.Waiting(doneChan, responnseChan, start, count, m.workerCount)
	}
}

func (m *mysql) Clean() {
	_, err := m.db.Query("drop table t")
	if err != nil {
		log.Fatalf("Clean table Error: %s", err)
	}
}

func (m *mysql) IsClean() bool {
	return m.isClean
}
