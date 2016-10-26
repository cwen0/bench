package main

import (
	"os"

	"github.com/cwen0/bench/cmd"
	"github.com/urfave/cli"
)

func main() {
	app := cli.NewApp()
	app.Name = "bench"
	app.Usage = "db bench test"
	app.Commands = []cli.Command{
		cmd.CmdMysql,
		cmd.CmdTiDB,
		cmd.CmdPrepare,
	}
	app.Run(os.Args)
}
