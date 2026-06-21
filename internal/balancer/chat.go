package balancer

import (
	"log/slog"
	"sync"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type chat struct {
	id string
	mu sync.Mutex
	mainTimeout time.Duration
	idleTimeout time.Duration
	lastActivity time.Time
	cancelChan chan struct{}

	client models.Conn
	clientErr error

	server models.Conn
	serverErr error
}

// tcpProxy проксирует TCP-трафик в режиме zero-copy
func (c *chat) tcpProxy() error {
	slog.Info("проксирование чата", "module", "chat", "chatId", c.id)
	defer slog.Info("проксирование чата завершено", "module", "chat", "chatId", c.id)
	c.setup()

	go c.processClient()
	c.processServer()

	if c.isClientErr() {
		return models.ErrClientSide
	}
	if c.isServerErr() {
		return c.getServerErr()	
	}

	return nil
}

func (c *chat) setup() {
	now := time.Now()
	c.setLastActivity(now)
	
	c.client.SetIdleTimeout(c.getIdleTimeout())
	c.server.SetIdleTimeout(c.getIdleTimeout())

	c.client.SetMainTimeout(c.getMainTimeout())
	c.server.SetMainTimeout(c.getMainTimeout())

	timer := time.NewTimer(c.getIdleTimeout())
	c.client.WithTimer(timer)
	c.server.WithTimer(timer)

	c.setCancelChan()
	c.client.WithCancel(c.getCancelChan())
	c.server.WithCancel(c.getCancelChan())
}

func (c *chat) processClient() {
	for {
		err := c.client.CopyTo(c.server)
		if err != nil {
			c.cancel()
			if err == models.ErrIdleTimeout {
				return
			}
			if err == models.ErrPollCancel {
				return
			}
			c.setClientErr(err)
			slog.Warn("ошибка на клиентской стороне", "module", "chat", "err", err)
			return
		}	
	}
}

func (c *chat) processServer() {
	for {
		err := c.server.CopyTo(c.client)
		if err != nil {
			c.cancel()
			if err == models.ErrIdleTimeout {
				return
			}
			if err == models.ErrPollCancel {
				return
			}
			c.setServerErr(err)
			slog.Warn("ошибка на серверной стороне", "module", "chat", "err", err)
			return
		}	
	}
}

// close останавливает чат, если его нужно остановить из вне (из балансировщика)
func (c *chat) close() {
	// c.stop()
	c.client.Close()
	c.server.Close()
}

func (c *chat) getIdleTimeout() time.Duration {
	return c.idleTimeout
}

func (c *chat) getMainTimeout() time.Duration {
	return c.mainTimeout
}

func (c *chat) setLastActivity(t time.Time) {
	c.lastActivity = t
}

func (c *chat) setCancelChan() {
	c.cancelChan = make(chan struct{})
}

func (c *chat) getCancelChan() chan struct{} {
	return c.cancelChan
}

func (c *chat) cancel() {
	// рекавер на случай двойного закрытия
	defer func ()  {
		if v := recover(); v != nil {
			return
		}	
	}()
	close(c.getCancelChan())
}

func (c *chat) setClientErr(err error) {
	c.mu.Lock()
	c.clientErr = err
	c.mu.Unlock()
}

func (c *chat) setServerErr(err error) {
	c.serverErr = err
}

func (c *chat) isClientErr() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientErr != nil
}

func (c *chat) isServerErr() bool {
	return c.serverErr != nil
}

func (c *chat) getServerErr() error {
	return c.serverErr
}