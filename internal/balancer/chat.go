package balancer

import (
	"runtime"
	"time"
)

type chat struct {
	id string
	idleTimeout time.Duration
	lastActivity time.Time
	client conn
	server conn
}

func (c *chat) tcpProxy() {
	c.client.LogActivity()
	c.server.LogActivity()

	now := time.Now()
	c.setLastActivity(now)
	c.client.SetLastActivity(now)
	c.server.SetLastActivity(now)
	
	c.client.SetIdleDeadline(now.Add(c.getIdleTimeout()))
	c.server.SetIdleDeadline(now.Add(c.getIdleTimeout()))
	go func() {
		for {
			err := c.client.CopyTo(c.server)
			if err != nil {
				// ??
				// также нужно иметь возможность тормознуть другой поток
			}
		}
	}()

	go func() {
		for {
			err := c.server.CopyTo(c.client)
			if err != nil {
				// ??
				// также нужно иметь возможность тормознуть другой поток
			}
		}
	}()

	for {
		clientLastActivity := c.client.LastActivity()
		if clientLastActivity.After(c.getLastActivity()) {
			c.setLastActivity(clientLastActivity)
			c.server.SetIdleDeadline(clientLastActivity.Add(c.getIdleTimeout()))
			continue
		}
		serverLastActivity := c.server.LastActivity()
		if serverLastActivity.After(c.getLastActivity()) {
			c.setLastActivity(serverLastActivity)
			c.client.SetIdleDeadline(serverLastActivity.Add(c.getIdleTimeout()))
		}
		runtime.Gosched()
	}
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