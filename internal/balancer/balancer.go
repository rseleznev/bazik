package balancer

import (
	"log"
	"sync"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/models"
)

type Balancer interface {
	Run()
}

type options struct {
	addr models.Address
	proto string // tcp

	// Алгоритм балансировки
	balancingAlg string
	
	retryAmount int
	maxClientsAmount int
	maxIdleSeconds int
	disableSocksPool bool
	maxSocksPoolLen int
	initialSocksPoolLen int
}

func NewBalancer(conf *config.Config, n networker) Balancer {
	switch conf.Proto {
	case "tcp":
		b := &TCPBalancer{
			opts: &options{
				addr: models.Address{
					IP: conf.IPbytes,
					Port: conf.Port,
				},
				proto: conf.Proto,
				balancingAlg: conf.BalancingAlg,
				retryAmount: conf.ServerOptions.RetryAmount,
				maxClientsAmount: conf.ServerOptions.MaxClientsAmount,
				maxIdleSeconds: conf.ServerOptions.MaxIdleSeconds,
				disableSocksPool: conf.ServerOptions.DisableSocksPool,
				maxSocksPoolLen: conf.ServerOptions.MaxSocksPoolLen,
				initialSocksPoolLen: conf.ServerOptions.InitialSocksPoolLen,
			},
			mu: sync.RWMutex{},
			servers: make([]*server, 0, len(conf.Servers)),
			chats: make(map[string]*chat, conf.ServerOptions.MaxClientsAmount),

			net: n,
		}
		for _, v := range conf.Servers {
			s := &server{
				net: n,
				addr: models.Address{
					Raw: v.Address,
					IP: v.IPbytes,
					Port: v.Port,
				},
				retryAmount: v.RetryAmount,
				maxClientsAmount: v.MaxClientsAmount,
				maxIdleSeconds: v.MaxIdleSeconds,
				disableSocksPool: v.DisableSocksPool,
				maxSocksPoolLen: v.MaxSocksPoolLen,
				initialSocksPoolLen: v.InitialSocksPoolLen,
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