package network

import (

	"github.com/rseleznev/bazik/internal/models"
)

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
	// создаем сокет

	// подключаемся
	
	return &socket{}, nil
}