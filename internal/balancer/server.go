package balancer

import (

	"github.com/rseleznev/bazik/internal/models"
)

type server struct {
	id string

	net networker
	
	addr models.Address
	activeConnectionsAmount int
	connPool chan conn

	retryAmount int
	maxResponseSeconds int
	maxClientsAmount int
	maxIdleSeconds int
	disableSocksPool bool
	maxSocksPoolLen int
	initialSocksPoolLen int
}

func (s *server) init() error {
	if !s.disableSocksPool {
		s.connPool = make(chan conn, s.maxSocksPoolLen)	
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

func (s *server) newConn() (conn, error) {
	c, err := s.net.NewConn(s.addr)
	if err != nil {
		return nil, err
	}

	// таймеры и настройки (TCP_NODELAY)

	return c, nil
}

func (s *server) getConn() (conn, error) {
	if s.disableSocksPool {
		c, err := s.newConn()
		if err != nil {
			return nil, err
		}
		return c, nil
	}
	
	if len(s.connPool) <= 0 {
		return nil, models.ErrNoConnsAvailable
	}
	c := <-s.connPool

	return c, nil
}

func (s *server) storeConn(c conn) {
	if s.disableSocksPool {
		return
	}
	if len(s.connPool) < s.maxSocksPoolLen {
		// нужно проверить неподтвержденные/непрочитанные данные

		s.connPool <- c
	}
}