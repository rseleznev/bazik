package balancer

import (
	"log/slog"
	"sync"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type chat struct {
	id string
	mainTimeout time.Duration
	idleTimeout time.Duration
	lastActivity time.Time
	ctlChan chan struct{}

	client models.Conn
	clientMu sync.Mutex
	clientCancelChan chan struct{}
	clientErr error

	server models.Conn
	serverMu sync.Mutex
	serverCancelChan chan struct{}
	serverErr error
}

// tcpProxy проксирует TCP-трафик в режиме zero-copy
func (c *chat) tcpProxy() error {
	slog.Info("проксирование чата", "module", "chat", "chatId", c.id)
	defer slog.Info("проксирование чата завершено", "module", "chat", "chatId", c.id)
	c.setup()
	
	timer := time.NewTimer(c.getIdleTimeout())
	for {
		select {
		case err := <-c.client.Readable():
			timer.Reset(c.getIdleTimeout())
			if err != nil {
				c.setClientErr(err)
				c.cancel()
				return c.handleError()
			}
			go c.copyFromClientToServer()
			continue

		case err := <-c.server.Readable():
			timer.Reset(c.getIdleTimeout())
			if err != nil {
				c.setServerErr(err)
				c.cancel()
				return c.handleError()
			}
			go c.copyFromServerToClient()
			continue

		case <-c.ctlChan:
			c.cancel()
			return c.handleError()

		case <-timer.C:
			c.cancel()
			return nil
		}
	}
}

func (c *chat) copyFromClientToServer() {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	defer func ()  {
		// рекавер на случай двойного закрытия ctlChan
		if r := recover(); r != nil {
			return
		}
	}()
	err := c.client.CopyTo(c.server)
	if err != nil {
		c.setClientErr(err)
		close(c.ctlChan)
	}
	
}

func (c *chat) copyFromServerToClient() {
	c.serverMu.Lock()
	defer c.serverMu.Unlock()
	defer func ()  {
		// рекавер на случай двойного закрытия ctlChan
		if r := recover(); r != nil {
			return
		}
	}()
	err := c.server.CopyTo(c.client)
	if err != nil {
		c.setServerErr(err)
		close(c.ctlChan)
	}
	
}

func (c *chat) setup() {
	now := time.Now()
	c.setLastActivity(now)

	c.client.SetTimeout(c.getMainTimeout())
	clientCancelChan := make(chan struct{})
	c.client.WithCancel(clientCancelChan)
	c.clientCancelChan = clientCancelChan

	c.server.SetTimeout(c.getMainTimeout())
	serverCancelChan := make(chan struct{})
	c.server.WithCancel(serverCancelChan)
	c.serverCancelChan = serverCancelChan
}

func (c *chat) handleError() error {
	if c.isClientErr() {
		return models.ErrClientSide
	}
	if c.isServerErr() {
		return c.getServerErr()
	}
	
	return nil
}

func (c *chat) cancel() {
	close(c.clientCancelChan)
	close(c.serverCancelChan)
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

func (c *chat) setClientErr(err error) {
	c.clientErr = err
}

func (c *chat) setServerErr(err error) {
	c.serverErr = err
}

func (c *chat) isClientErr() bool {
	return c.clientErr != nil
}

func (c *chat) isServerErr() bool {
	return c.serverErr != nil
}

func (c *chat) getServerErr() error {
	return c.serverErr
}