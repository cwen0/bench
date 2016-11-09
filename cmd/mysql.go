package cmd

import (
	"github.com/cwen0/bench/lib/mysql"
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
		boolFlag("transcation,t", "transcation commit"),
		boolFlag("clean", "clean test table"),
	},
}

func runMysql(ctx *cli.Context) error {
	mql := mysql.NewMysql(ctx)
	mql.ReadTestData()
	mql.OpenDB()
	defer mql.CloseDB()
	mql.Test()
	if mql.IsClean() {
		mql.Clean()
	}
	return nil
}
