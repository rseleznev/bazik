package main

type Address struct {
	ip [4]byte
	port int
}

type Server struct {
	Addr Address
	ConnectionAmount int
	Weight int
}

type Balancer struct {
	conf *Config
	servers []*Server
}

func NewBalancer(c *Config) *Balancer {
	return &Balancer{
		conf: c,
	}
}

func (b *Balancer) findServer() *Server {
	return b.servers[0]
}