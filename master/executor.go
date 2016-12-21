package master

import (
	"net"
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
		e.logger.Info("Shutting down master executor on endpoint %s", e.addr)
		wg.Done()
	}()

	e.exit = exit
	e.logger.Info("Starting master executor at endpoint %s", e.addr)

	ln, err := net.ListenTCP("tcp", e.addr)
	if err != nil {
		e.logger.Error("Failed to listen on %s", e.addr)
		return
	}

	for !*exit {
		ln.SetDeadline(time.Now().Add(1e9))
		conn, err := ln.Accept()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				continue
			}
			e.logger.Warning(err.Error())
		}
		go e.handleConnection(conn)
	}
}

func (e *executor) handleConnection(conn net.Conn) {
	defer func() {
		e.logger.Info("Closing connection to %s", conn.RemoteAddr().String())
		conn.Close()
	}()

	e.logger.Info("Received connection from %s", conn.RemoteAddr().String())

	for !*e.exit {
		n, err := conn.Write([]byte(time.Now().String() + "\n"))
		if err != nil {
			e.logger.Error("Failed to write to connection at %s: %s", conn.RemoteAddr(), err.Error())
			return
		}
		e.logger.Info("Wrote %d bytes to slave at %s", n, conn.RemoteAddr())
		time.Sleep(time.Second)
	}
}
