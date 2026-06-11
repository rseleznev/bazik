package main

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/balancer"
	"github.com/rseleznev/bazik/internal/network"
	"github.com/rseleznev/bazik/polling"
)

func serverInit(addr string) (net.Listener, error) {
	listener, err := net.Listen("tcp4", addr)
	if err != nil {
		return nil, err
	}
	return listener, err
}

func serverRun(l net.Listener) error {
	conn, err := l.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()
	buf := make([]byte, 1024)
	msgNum := 1
	for msgNum < 11 {
		n, err := conn.Read(buf)
		if err != nil {
			return err
		}
		fmt.Println("прочитано сервером: ", string(buf[:n]))

		_, err = conn.Write([]byte(fmt.Sprintf("server message %d", msgNum)))
		if err != nil {
			return err
		}
		msgNum++
	}
	return nil
}

func balancerRun() error {
	poller, err := polling.NewEpoll()
	if err != nil {
		return err
	}
	net := network.NewNet(poller)

	conf := config.Parse("./config/config_sample.yaml")
	b := balancer.NewBalancer(conf[0].Balancer, conf[0].Servers, net)
	if b == nil {
		return errors.New("ошибка создания балансера")
	}
	go b.Run()
	return nil
}

func clientRun(ip [4]byte, port int) error {
	conn, err := net.DialTCP("tcp", nil, &net.TCPAddr{IP: net.IPv4(ip[0], ip[1], ip[2], ip[3]), Port: port})
	if err != nil {
		return err
	}
	defer conn.Close()
	buf := make([]byte, 1024)
	msgNum := 1
	for msgNum < 11 {
		_, err := conn.Write([]byte(fmt.Sprintf("client message %d", msgNum)))
		if err != nil {
			return err
		}

		n, err := conn.Read(buf)
		if err != nil {
			return err
		}
		fmt.Println("прочитано клиентом: ", string(buf[:n]))
		msgNum++
	}
	return nil
}

func TestSimpleProxy(t *testing.T) {
	wg := sync.WaitGroup{}
	listener, err := serverInit("127.0.0.1:9000")
	if err != nil {
		t.Error(err)
	}
	defer listener.Close()
	wg.Go(func() {
		err := serverRun(listener)
		if err != nil {
			t.Error(err)
		}
	})

	err = balancerRun()
	if err != nil {
		t.Error(err)
	}
	time.Sleep(time.Millisecond*50)

	wg.Go(func() {
		err := clientRun([4]byte{127, 0, 0, 1}, 5000)
		if err != nil {
			t.Error(err)
		}
	})
	wg.Wait()
}