package network

import (
	"syscall"

	"github.com/rseleznev/bazik/internal/models"
)

type syscaller interface {
	NewSocket(int, int, int) (int, error)
	CloseSocket(int) error
	Bind(int, syscall.Sockaddr) error
	Listen(int, int) error
	Accept(int) (int, syscall.Sockaddr, error)
	Connect(int, syscall.Sockaddr) error
	Splice(writer, reader int) (int64, error)
	Pipe() (int, int, error)
}

type poller interface {
	Add(models.PollingUnit) error
	DeleteSocketFromPolling(int)
}

type Net struct {
	sys syscaller
	poller poller
}

func (n *Net) NewTCPListener() (*socket, error) {
	return &socket{}, nil
}

func (n *Net) NewConn(addr models.Address) (*socket, error) {
	return &socket{}, nil
}