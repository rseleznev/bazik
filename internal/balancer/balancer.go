package balancer

import (
	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/models"
)

type Balancer interface {
	Start()
}

type options struct {
	addr string // 127.0.0.1:5000
	proto string // tcp

	// Алгоритм балансировки
	balancingAlg string
	
	config.ServerOptions
}

type handler interface {
	InitServer(models.Server)
	Listen(addr models.Address)
	Accept() *models.Client
	Close(*models.Client)
	TCPProxy(*models.Client, models.Server) error
}

func NewBalancer(conf *config.Config, h handler) Balancer {
	var o options

	if conf.Proto == "tcp" {
		h, ok := h.(tcpHandler)
		if !ok {
			panic("balancer interface assert err")
		}
		
		return &TCPBalancer{
			opts: &o,
			clients: make(map[int]*models.Client, o.MaxClientsAmount),

			handler: h,
		}	
	}

	return &TCPBalancer{}
}