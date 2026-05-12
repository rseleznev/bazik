package handler

import "syscall"

type realSyscalls struct {
}

func (r realSyscalls) NewSocket(d int, t int, p int) (int, error) {
	return syscall.Socket(d, t, p)
}

func (r realSyscalls) CloseSocket(sock int) error {
	return syscall.Close(sock)
}

func (r realSyscalls) Bind(sock int, addr syscall.Sockaddr) error {
	return syscall.Bind(sock, addr)
}

func (r realSyscalls) Listen(sock int, q int) error {
	return syscall.Listen(sock, q)
}

func (r realSyscalls) Accept(sock int) (int, syscall.Sockaddr, error) {
	return syscall.Accept(sock)
}

func (r realSyscalls) Connect(sock int, addr syscall.Sockaddr) error {
	return syscall.Connect(sock, addr)
}

func (r realSyscalls) Splice(writer, reader int) (int64, error) { // возможно что-то перепутано
	return syscall.Splice(writer, nil, reader, nil, 10, 0) // длину нужно откуда-то брать
}

func (r realSyscalls) Pipe() (int, int, error) {
	fds := []int{}
	err := syscall.Pipe2(fds, syscall.O_NONBLOCK)
	
	return fds[0], fds[1], err
}