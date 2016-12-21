package slave

import (
	"bufio"
	"io"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/jseely/logging"
)

type executor struct {
	logger *logging.LoggingContext
	addr   *net.TCPAddr
	exit   *bool
}

func NewExecutor(logger *logging.LoggingContext, endpoint string) *executor {
	addr, err := net.ResolveTCPAddr("tcp", endpoint)
	if err != nil {
		logger.Error(err.Error())
		panic(err.Error())
	}
	return &executor{
		logger: logger,
		addr:   addr,
	}
}

func (e *executor) Run(exit *bool, wg *sync.WaitGroup) {
	defer func() {
		e.logger.Info("Shutting down slave communicating with master at %s", e.addr)
		wg.Done()
	}()

	e.exit = exit
	e.logger.Info("Starting slave to master at %s", e.addr)

	for !*exit {
		conn, err := net.DialTCP("tcp", nil, e.addr)
		if err != nil {
			e.logger.Error("Failed to connect to master at %s. Waiting...", e.addr)
			time.Sleep(time.Second * 1)
			continue
		}

		e.handleConnection(conn)
	}
}

func (e *executor) handleConnection(conn *net.TCPConn) {
	defer func() {
		e.logger.Info("Closing connection to %s", e.addr)
		conn.Close()
	}()

	err := conn.SetKeepAlive(true)
	if err != nil {
		e.logger.Error("Failed to set keep alive on the connection to %s", e.addr)
		return
	}

	reader := bufio.NewReader(conn)
	for !*e.exit {
		message, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return
			}
			e.logger.Error(err.Error())
			continue
		}
		e.logger.Info("Received message from master at %s: %s", e.addr, strings.TrimSpace(message))
	}
}
