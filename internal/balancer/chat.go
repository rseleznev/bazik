package balancer

type chat struct {
	id string
	client conn
	server conn
}

func (c *chat) tcpProxy() {
	for {
		err := c.client.CopyTo(c.server)
		if err != nil {
			// ??
		}

		err = c.server.CopyTo(c.client)
		if err != nil {
			// ??
		}
	}
}

func (c *chat) tcpPipeline() {
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