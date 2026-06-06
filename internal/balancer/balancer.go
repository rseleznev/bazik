package balancer

import (
	"log/slog"
	"os"
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
				slog.Error("ошибка инициализации сервера", "module", "balancer", "serverAddr", s.opts.Addr.Raw, "err", err)
				os.Exit(1)
			}
			b.servers = append(b.servers, s)
		}
		slog.Info("создан TCPBalancer", "module", "balancer")

		return b

	default:
		return nil

	}
}