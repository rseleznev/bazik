package polling

import "syscall"

type epollSyscalls interface {
	Wait(int, []syscall.EpollEvent, int) (int, error)
	GetSocketOpt(int, int, int) (int, error)
	Ctl(int, int, int, *syscall.EpollEvent) error
}

type epollRealSyscalls struct {}

func (es epollRealSyscalls) Wait(eFd int, events []syscall.EpollEvent, timeout int) (int, error) {
	return syscall.EpollWait(eFd, events, timeout)
}

func (es epollRealSyscalls) GetSocketOpt(sFd, l, o int) (int, error) {
	return syscall.GetsockoptInt(sFd, l, o)
}

func (es epollRealSyscalls) Ctl(eFd, o, sFd int, event *syscall.EpollEvent) error {
	return syscall.EpollCtl(eFd, o, sFd, event)
}