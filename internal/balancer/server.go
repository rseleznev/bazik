package balancer

import (
	"sync/atomic"

	"github.com/rseleznev/bazik/internal/models"
)

type server struct {
	addr models.Address
	activeConnectionsAmount atomic.Int32
	connPool chan models.Conn

	net networker

	retryAmount int
	maxClientsAmount int
	maxIdleSeconds int
	disableSocksPool bool
	maxSocksPoolLen int
	initialSocksPoolLen int
}

func (s *server) init() error {
	if !s.disableSocksPool {
		s.connPool = make(chan models.Conn, s.maxSocksPoolLen)	
	}
	
	for range s.initialSocksPoolLen {
		c, err := s.newConn()
		if err != nil {
			return err
		}
		s.storeConn(c)
	}
	return nil
}

func (s *server) newConn() (models.Conn, error) {
	c, err := s.net.NewConn(s.addr)
	if err != nil {
		return nil, err
	}

	// таймеры и настройки (TCP_NODELAY)

	return c, nil
}

func (s *server) getConn() (models.Conn, error) {
	if s.disableSocksPool {
		if int(s.activeConnectionsAmount.Load()) < s.maxClientsAmount {
			c, err := s.newConn()
			if err != nil {
				return nil, err
			}
			s.activeConnectionsAmount.Add(1)
			return c, nil
		}
		return nil, models.ErrNoConnsAvailable
	}
	
	if len(s.connPool) <= 0 {
		if int(s.activeConnectionsAmount.Load()) < s.maxClientsAmount {
			c, err := s.newConn()
			if err != nil {
				return nil, err
			}
			s.activeConnectionsAmount.Add(1)
			return c, nil
		}
		return nil, models.ErrNoConnsAvailable
	}
	s.activeConnectionsAmount.Add(1)

	return <-s.connPool, nil
}

func (s *server) storeConn(c models.Conn) {
	s.activeConnectionsAmount.Add(-1)
	if s.disableSocksPool {
		return
	}
	if len(s.connPool) < s.maxSocksPoolLen {
		n, err := c.CheckUnread()
		if err != nil {
			// логируем ошибку
		}
		if n != 0 {
			return
		}

		n, err = c.CheckUnsent()
		if err != nil {
			// логируем ошибку
		}
		if n != 0 {
			return
		}

		s.connPool <- c
	}
}