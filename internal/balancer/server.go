package balancer

import (
	"sync/atomic"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type server struct {
	opts *models.ServerOptions
	activeConnectionsAmount atomic.Int32
	connPool chan models.Conn

	net networker
}

func (s *server) init() error {
	if !s.opts.DisableSocksPool {
		s.connPool = make(chan models.Conn, s.opts.MaxSocksPoolLen)	
	}
	
	for range s.opts.InitialSocksPoolLen {
		c, err := s.newConn()
		if err != nil {
			return err
		}
		s.storeConn(c)
	}
	return nil
}

func (s *server) newConn() (models.Conn, error) {
	c, err := s.net.NewConn(s.opts.Addr)
	if err != nil {
		return nil, err
	}

	// таймеры и настройки (TCP_NODELAY)

	return c, nil
}

func (s *server) getConn() (models.Conn, error) {
	if s.opts.DisableSocksPool {
		if int(s.activeConnectionsAmount.Load()) < s.opts.MaxClientsAmount {
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
		if int(s.activeConnectionsAmount.Load()) < s.opts.MaxClientsAmount {
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
	if s.opts.DisableSocksPool {
		return
	}
	if len(s.connPool) < s.opts.MaxSocksPoolLen {
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

func (s *server) getIdleTimeout() time.Duration {
	return time.Duration(s.opts.MaxIdleSeconds)*time.Second
}

func (s *server) getMainTimeout() time.Duration {
	return time.Duration(s.opts.MainTimeout)*time.Millisecond
}