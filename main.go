package main

import (
	"os"
	"sync"
	"time"

	"github.com/jseely/project-e/master"
	"github.com/jseely/project-e/slave"

	"github.com/jseely/logging"
	flag "github.com/jseely/pflag"
)

var fListEnd string
var fRemoteEnd string

func init() {
	flag.StringVarP(&fListEnd, "listen", "l", ":7444", "The endpoint this process should listen on")
	flag.StringVarP(&fRemoteEnd, "remote", "r", "", "The endpoint of a machine to connect to as a slave. If empty, process will only be a master.")
	flag.Parse()
}

func main() {
	var wg sync.WaitGroup
	exit := false

	logger := &logging.LoggingContext{
		LogLevel: logging.Debug,
		Writer:   os.Stdout,
	}

	m := master.NewExecutor(logger.Copy().SetPrefix("master."), fListEnd)

	wg.Add(1)
	go m.Run(&exit, &wg)

	if fRemoteEnd != "" {
		s := slave.NewExecutor(logger.Copy().SetPrefix("slave."), fRemoteEnd)
		wg.Add(1)
		go s.Run(&exit, &wg)
	}

	time.Sleep(time.Second * 10)

	exit = true

	wg.Wait()

}
