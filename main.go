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

	conf := config.Parse("./config/config_sample.yaml")
	b := balancer.NewBalancer(conf[0].Balancer, conf[0].Servers, net)
	b.Run()
}