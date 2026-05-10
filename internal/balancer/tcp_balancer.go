package balancer

import (
	"math/rand"
	"sync"

	"github.com/rseleznev/bazik/internal/models"
)


type tcpHandler interface {
	InitServer(models.Server)
	Listen(addr models.Address)
	Accept() *models.Client
	Close(*models.Client)
	TCPProxy(*models.Client, models.Server) error
}

type TCPBalancer struct {
	mu sync.Mutex
	opts *options
	servers []*server
	clients map[int]*models.Client
	clientsAmount int

	handler tcpHandler
}

func (b *TCPBalancer) Start() {
	for _, s := range b.servers {
		b.handler.InitServer(s)
	}
	
	for {
		client := b.handler.Accept()
		go b.processNewClient(client)

		continue
	}
}

func (b *TCPBalancer) processNewClient(client *models.Client) {
	b.mu.Lock()

	if b.clientsAmount+1 >= b.opts.MaxClientsLimit {
		b.mu.Unlock()
		b.handler.Close(client)

		return
	}
	b.clientsAmount++
	b.clients[client.Sock] = client

	// подбираем сервер для клиента
	s := b.findServer()
	b.mu.Unlock()

	err := b.handler.TCPProxy(client, s)
	if err != nil {
		// если сервер упал - выдаем новый

		// если клиент не ответил за отведенный таймаут?
	}	
}

func (b *TCPBalancer) findServer() *server {
	switch b.opts.balancingAlg {
	case "random":
		n := rand.Intn(len(b.servers))
		return b.servers[n]

	default:
		return &server{}

	}
}