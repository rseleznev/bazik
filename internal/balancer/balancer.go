package balancer

import (
	"log"
	"sync"

	"github.com/rseleznev/bazik/internal/models"
)

type Balancer interface {
	Run()
}

func NewBalancer(opts *models.BalancerOptions, servers []*models.ServerOptions, n networker) Balancer {
	switch opts.Proto {
	case "tcp":
		b := &TCPBalancer{
			opts: opts,
			mu: sync.RWMutex{},
			servers: make([]*server, 0, len(servers)),
			chats: make(map[string]*chat, opts.MaxClientsAmount),

			net: n,
		}
		for _, o := range servers {
			s := &server{
				opts: o,
				net: n,
			}
			err := s.init()
			if err != nil {
				log.Fatal(err)
			}
			b.servers = append(b.servers, s)
		}
		return b

	default:
		return nil

	}
}