package network

import (
	"sync"

	"github.com/rseleznev/bazik/internal/models"
)

type poller interface {
	Add(models.PollingUnit) error
	DeleteSocketFromPolling(int)
}

type net struct {
	sys syscaller
	poller poller
}

func NewNet(p poller) *net {
	return &net{
		sys: &realSyscalls{
			mu: sync.Mutex{},
			pipeFDs: make([]int, 2),
		},
		poller: p,
	}
}

func (n *net) NewTCPListener() (models.Conn, error) {
	return &socket{}, nil
}

func (n *net) NewConn(addr models.Address) (models.Conn, error) {
	// создаем сокет

	// подключаемся
	
	return &socket{}, nil
}