package main

import (
	"log"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/balancer"
	"github.com/rseleznev/bazik/internal/network"
	"github.com/rseleznev/bazik/polling"
)

func main() {
	poller, err := polling.NewEpoll()
	if err != nil {
		log.Fatal(err)
	}
	net := network.NewNet(poller)

	conf := &config.Config{
		IPbytes: [4]byte{127, 0, 0, 1},
		Port: 9000,
		Proto: "tcp",
		BalancingAlg: "random",
		ProxyMode: "zero-copy",
		MainTimeout: 500,
		ServerOptions: config.ServerOptions{
			RetryAmount: 0,
			MaxClientsAmount: 3,
			MaxIdleSeconds: 300,
			MaxSocksPoolLen: 10,
			InitialSocksPoolLen: 3,
		},
		Servers: []struct{
			Address string
			IPbytes [4]byte
			Port int
			config.ServerOptions
		}{
			{
				IPbytes: [4]byte{127, 0, 0, 1},
				Port: 6379,

				ServerOptions: config.ServerOptions{
					RetryAmount: 0,
					MaxClientsAmount: 3,
					MaxIdleSeconds: 300,
					MaxSocksPoolLen: 10,
					InitialSocksPoolLen: 3,
				},
			},
		},
	}
	b := balancer.NewBalancer(conf, net)
	b.Run()
}