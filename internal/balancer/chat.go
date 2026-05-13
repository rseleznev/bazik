package balancer

type chat struct {
	id string
	client conn
	server conn
}

func (c *chat) tcpProxy() {
	go func() {
		for {
			err := c.client.CopyTo(c.server)
			if err != nil {
				// ??
			}	
		}
	}()

	go func() {
		for {
			err := c.server.CopyTo(c.client)
			if err != nil {
				// ??
			}
		}
	}()
}