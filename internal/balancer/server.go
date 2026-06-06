package balancer

import (
	"log/slog"
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
	slog.Info("инициализация сервера", "module", "server")
	if s.opts.DisableConnsPool {
		slog.Info("инициализация сервера без пула успешно завершена", "module", "server")
		return nil
	}
	s.connPool = make(chan models.Conn, s.opts.MaxConnsPoolLen)

	for range s.opts.InitialConnsPoolLen {
		c, err := s.newConn()
		if err != nil {
			return err
		}
		s.connPool <- c
	}
	slog.Info("инициализация сервера с пулом успешно завершена", "module", "server")
	
	return nil
}

func (s *server) newConn() (models.Conn, error) {
	c, err := s.net.NewTCPConn(s.opts.Addr)
	if err != nil {
		return nil, err
	}
	c.SetMainTimeout(time.Duration(s.opts.MainTimeout)*time.Millisecond)
	err = c.Connect()
	if err != nil {
		return nil, err
	}

	// таймеры и настройки (TCP_NODELAY)

	return c, nil
}

func (s *server) getConn() (models.Conn, error) {
	if s.opts.DisableConnsPool {
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
	if s.opts.DisableConnsPool {
		return
	}
	if len(s.connPool) < s.opts.MaxConnsPoolLen {
		n, err := c.CheckUnread()
		if err != nil {
			slog.Error("ошибка вызова CheckUnread", "module", "server", "serverAddr", s.opts.Addr.Raw, "err", err)
		}
		if n != 0 {
			slog.Warn("непрочитанные данные сервера", "module", "server", "serverAddr", s.opts.Addr.Raw, "num", n)
			return
		}

		n, err = c.CheckUnsent()
		if err != nil {
			slog.Error("ошибка вызова CheckUnsent", "module", "server", "serverAddr", s.opts.Addr.Raw, "err", err)
		}
		if n != 0 {
			slog.Warn("неотправленные данные сервера", "module", "server", "serverAddr", s.opts.Addr.Raw, "num", n)
			return
		}

		s.connPool <- c
	}
}

func (s *server) getIdleTimeout() time.Duration {
	return time.Duration(s.opts.MaxIdleSeconds)*time.Second
}