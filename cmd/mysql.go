package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

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
	},
}

type mysql struct {
	user     string
	password string
	host     string
	port     string
	dbName   string
	casePath string
	testData []string
	db       *sql.DB
}

func newMysql(ctx *cli.Context) *mysql {
	if !ctx.IsSet("case-path") {
		log.Fatal("Argument case-path is must")
	}
	return &mysql{
		user:     ctx.String("username"),
		password: ctx.String("password"),
		host:     ctx.String("host"),
		port:     ctx.String("port"),
		dbName:   ctx.String("database"),
		casePath: ctx.String("case-path"),
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
	start := time.Now()
	defer func() {
		elapsed := time.Since(start)
		log.Printf("Test case cost: %s", elapsed)
	}()
	for _, sql := range m.testData {
		if _, err := m.db.Query(sql); err != nil {
			log.Fatalf("Exec case Error: %s", err)
		}
	}
}

func runMysql(ctx *cli.Context) error {
	mql := newMysql(ctx)
	mql.readTestData()
	mql.openDB()
	mql.test()
	return nil
}
