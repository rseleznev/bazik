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

func (s *Server) Connect() int {
	serverSocket := 0
	s.ConnectionAmount++

	return serverSocket
}