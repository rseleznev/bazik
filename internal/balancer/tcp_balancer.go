package balancer

import (
	"math/rand"
	"sync/atomic"

	"github.com/rseleznev/bazik/internal/models"
)


type tcpHandler interface {
	Accept() *models.Client
	TCPProxy(*models.Client, *models.Server) error
}

type TCPBalancer struct {
	opts *options
	servers []*models.Server
	clientsLen atomic.Int32

	handler tcpHandler
}

func (b *TCPBalancer) Start() {
	for {
		client := b.handler.Accept()
		go b.processNewClient(client)

		continue
	}
}

func (b *TCPBalancer) processNewClient(client *models.Client) {
	// подбираем сервер для клиента
	s := b.findServer()

	err := b.handler.TCPProxy(client, s)
	if err != nil {
		
	}	
}

func (b *TCPBalancer) findServer() *models.Server {
	switch b.opts.balancingAlg {
	case "random":
		n := rand.Intn(len(b.servers))
		return b.servers[n]

	default:
		return &models.Server{}

	}
}