package main

import (
	"log/slog"
	"os"

	"github.com/rseleznev/bazik/config"
	"github.com/rseleznev/bazik/internal/balancer"
	"github.com/rseleznev/bazik/internal/network"
	"github.com/rseleznev/bazik/polling"
)

func main() {
	poller, err := polling.NewEpoll()
	if err != nil {
		slog.Error("ошибка создания поллера", "module", "main", "err", err)
		os.Exit(1)
	}
	slog.Info("поллер создан", "module", "main")
	net := network.NewNet(poller)

	conf := config.Parse("./config/config_sample.yaml")
	b := balancer.NewBalancer(conf[0].Balancer, conf[0].Servers, net) // временная передача по индексу
	if b == nil {
		slog.Error("ошибка создания балансера", "module", "main", "err", err)
		os.Exit(1)
	}
	b.Run()
}