package balancer

import (
	"runtime"
	"sync"
	"time"

	"github.com/rseleznev/bazik/internal/models"
)

type chat struct {
	id string
	mu sync.RWMutex
	availableRetries int
	paused bool
	ended bool
	idleTimeout time.Duration
	lastActivity time.Time

	client conn
	server conn

	clientErr error
	serverErr error
	serverErrsOccurred int
}

func (c *chat) isPaused() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.paused
}

func (c *chat) pause() {
	c.mu.Lock()
	if !c.paused {
		c.paused = true
	}
	c.mu.Unlock()
}

func (c *chat) isEnded() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ended
}

func (c *chat) end() {
	c.mu.Lock()
	if !c.ended {
		c.ended = true
	}
	c.mu.Unlock()
}

func (c *chat) tcpProxy() (int, error) {
	if c.serverErrsOccurredAmount() >= 3 {
		// ограничение, чтобы не получить бесконечный цикл,
		// когда клиент получает ошибку на нескольких серверах

		// нужно решить, кто будет следить за доступными ретраями
	}
	
	c.client.LogActivity()
	c.server.LogActivity()

	now := time.Now()
	c.setLastActivity(now)
	c.client.SetLastActivity(now)
	c.server.SetLastActivity(now)
	
	c.client.SetIdleDeadline(now.Add(c.getIdleTimeout()))
	c.server.SetIdleDeadline(now.Add(c.getIdleTimeout()))
	go func() {
		for !c.isPaused() && !c.isEnded() {
			err := c.client.CopyTo(c.server)
			if err != nil {
				if err == models.ErrIdleTimeout {
					c.end()
					return
				}
				c.setClientErr(err)
				c.pause()
				break
			}
		}
	}()

	go func() {
		for !c.isPaused() && !c.isEnded() {
			err := c.server.CopyTo(c.client)
			if err != nil {
				if err == models.ErrIdleTimeout {
					c.end()
					return
				}
				c.setServerErr(err)
				c.pause()
				break
			}
		}
	}()

	for !c.isEnded() {
		if c.isPaused() {
			if c.isClientErr() {
				// в случае клиентской ошибки нам нечего делать с клиентом,
				// поэтому мы просто считаем соединение (чат) завершенным
				c.client.Close()

				// серверный сокет не закрываем, т.к. можем вернуть его в пул
				// и переиспользовать
				break
			}
			c.server.Close()
			c.serverErrOccurred()
			return c.getAvailableRetries(), c.getServerErr()
		}
		
		clientLastActivity := c.client.LastActivity()
		if clientLastActivity.After(c.getLastActivity()) {
			c.setLastActivity(clientLastActivity)
			c.server.SetIdleDeadline(clientLastActivity.Add(c.getIdleTimeout()))
			runtime.Gosched()
			continue
		}
		serverLastActivity := c.server.LastActivity()
		if serverLastActivity.After(c.getLastActivity()) {
			c.setLastActivity(serverLastActivity)
			c.client.SetIdleDeadline(serverLastActivity.Add(c.getIdleTimeout()))
		}
		runtime.Gosched()
	}

	return c.getAvailableRetries(), nil
}

// close останавливает чат, если его нужно остановить из вне (из балансировщика)
func (c *chat) close() {
	c.end()
	c.client.Close()
	c.server.Close()
}

func (c *chat) getIdleTimeout() time.Duration {
	return c.idleTimeout
}

func (c *chat) getLastActivity() time.Time {
	return c.lastActivity
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

func (c *chat) getServerErr() error {
	return c.serverErr
}

func (c *chat) serverErrOccurred() {
	c.serverErrsOccurred++
}

func (c *chat) serverErrsOccurredAmount() int {
	return c.serverErrsOccurred
}

func (c *chat) getAvailableRetries() int {
	return c.availableRetries - c.serverErrsOccurred
}