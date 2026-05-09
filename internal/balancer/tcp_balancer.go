package balancer

import (
	"github.com/rseleznev/bazik/internal/models"
)


type tcpHandler interface {
	Listen(chan *models.Client)
	TCPProxy(*models.Client, *models.Server) error
}

type TCPBalancer struct {
	opts *options
	servers []*models.Server
	newClients chan *models.Client

	handler tcpHandler
}

func (b *TCPBalancer) Start() {
	go b.handler.Listen(b.newClients)

	for client := range b.newClients {
		go b.processNewClient(client)
	}
}

func (b *TCPBalancer) processNewClient(client *models.Client) {
	// подбирает сервер для клиента
	s := b.servers[0]

	err := b.handler.TCPProxy(client, s)
	if err != nil {
		
	}
}