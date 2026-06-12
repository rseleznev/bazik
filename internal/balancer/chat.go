package balancer

import (
	"log/slog"
	"sync"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type chat struct {
	id string
	mu sync.RWMutex
	// paused bool
	stopped bool
	mainTimeout time.Duration
	idleTimeout time.Duration
	lastActivity time.Time
	ctlChan chan struct{}

	client models.Conn
	server models.Conn

	clientErr error
	serverErr error
}

// func (c *chat) isPaused() bool {
// 	c.mu.RLock()
// 	defer c.mu.RUnlock()
// 	return c.paused
// }

// func (c *chat) pause() {
// 	c.mu.Lock()
// 	if !c.paused {
// 		c.paused = true
// 	}
// 	c.mu.Unlock()
// }

func (c *chat) isStopped() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.stopped
}

func (c *chat) stop() {
	c.mu.Lock()
	if !c.stopped {
		c.stopped = true
		close(c.ctlChan)
	}
	c.mu.Unlock()
}

// tcpProxy проксирует TCP-трафик в режиме zero-copy
func (c *chat) tcpProxy() error {
	slog.Info("проксирование чата", "module", "chat", "chatId", c.id)
	defer slog.Info("проксирование чата завершено", "module", "chat", "chatId", c.id)
	c.setup()

	go func() {
		for !c.isStopped() {
			err := c.client.CopyTo(c.server)
			if err != nil {
				if err == models.ErrIdleTimeout {
					c.stop()
					return
				}
				c.setClientErr(err)
				slog.Warn("ошибка на клиентской стороне", "module", "chat", "err", err)
				c.stop()
				return
			}
		}
	}()

	go func() {
		for !c.isStopped() {
			err := c.server.CopyTo(c.client)
			if err != nil {
				if err == models.ErrIdleTimeout {
					c.stop()
					return
				}
				c.setServerErr(err)
				slog.Warn("ошибка на серверной стороне", "module", "chat", "err", err)
				c.stop()
				return
			}
		}
	}()

outer:
	for {
		select {
		case clientLastActivity := <-c.client.LastActivity():
			c.setLastActivity(clientLastActivity)
			c.server.SetIdleDeadline(clientLastActivity.Add(c.getIdleTimeout()))
			continue

		case serverLastActivity := <-c.server.LastActivity():
			c.setLastActivity(serverLastActivity)
			c.client.SetIdleDeadline(serverLastActivity.Add(c.getIdleTimeout()))
			continue

		case <-c.ctlChan:
			break outer
		}
	}
	if c.isClientErr() {
		return models.ErrClientSide
	}
	if c.isServerErr() {
		c.server.Close()
		return c.getServerErr()	
	}
	c.client.Close()

	return nil
}

func (c *chat) setup() {
	c.client.LogActivity()
	c.server.LogActivity()

	now := time.Now()
	c.setLastActivity(now)
	// c.client.SetLastActivity(now)
	// c.server.SetLastActivity(now)
	
	c.client.SetIdleDeadline(now.Add(c.getIdleTimeout()))
	c.server.SetIdleDeadline(now.Add(c.getIdleTimeout()))

	c.client.SetMainTimeout(c.getMainTimeout())
	c.server.SetMainTimeout(c.getMainTimeout())
}

// close останавливает чат, если его нужно остановить из вне (из балансировщика)
func (c *chat) close() {
	c.stop()
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