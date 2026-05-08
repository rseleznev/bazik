package balancer

import (
	"math/rand"
	"sync"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/models"
)

type server struct {
	mu sync.Mutex
	connectionsLen int

	config.ParsedServer
}

type Balancer struct {
	balancingAlg string
	servers []*server
}

func NewBalancer(c *config.Config) *Balancer {
	servers := make([]*server, 5)
	
	return &Balancer{
		balancingAlg: c.BalancingAlg,
		servers: servers,
	}
}

func (b *Balancer) FindServer() models.Server {
	switch b.balancingAlg {
	case "random":
		n := rand.Intn(len(b.servers))
		
		return b.servers[n]

	default:
		return b.servers[0]

	}
}

func (s *server) GetAddr() models.Address {
	return s.Addr
}