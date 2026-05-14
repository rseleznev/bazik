package network

import (
	"sync"
	"syscall"
)

const (
	SPLICE_F_MORE = 0x4
	SPLICE_F_MOVE = 0x1
	SPLICE_F_NONBLOCK = 0x2
)

type syscaller interface {
	NewSocket(int, int, int) (int, error)
	Close(int) error
	Bind(int, syscall.Sockaddr) error
	Listen(int, int) error
	Accept(int) (int, syscall.Sockaddr, error)
	Connect(int, syscall.Sockaddr) error
	Splice(writer, reader int) (int64, error)
	Pipe() (int, int, error)
}

type realSyscalls struct {
	mu sync.Mutex

	// буфер для создания дескрипторов пайпа,
	// чтобы избежать частых мелких аллокаций
	pipeFDs []int
}

func (r *realSyscalls) NewSocket(d int, t int, p int) (int, error) {
	return syscall.Socket(d, t, p)
}

func (r *realSyscalls) Close(fd int) error {
	return syscall.Close(fd)
}

func (r *realSyscalls) Bind(sock int, addr syscall.Sockaddr) error {
	return syscall.Bind(sock, addr)
}

func (r *realSyscalls) Listen(sock int, q int) error {
	return syscall.Listen(sock, q)
}

func (r *realSyscalls) Accept(sock int) (int, syscall.Sockaddr, error) {
	return syscall.Accept(sock)
}

func (r *realSyscalls) Connect(sock int, addr syscall.Sockaddr) error {
	return syscall.Connect(sock, addr)
}

func (r *realSyscalls) Splice(writer, reader int) (int64, error) { // возможно что-то перепутано
	return syscall.Splice(writer, nil, reader, nil, 16*1024, 0) // буфер 16Кб
}

func (r *realSyscalls) Pipe() (int, int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	err := syscall.Pipe2(r.pipeFDs, syscall.O_NONBLOCK)
	
	return r.pipeFDs[0], r.pipeFDs[1], err
}