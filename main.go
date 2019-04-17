package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/bitmark-inc/bitmarkd/zmqutil"
	"github.com/jamieabc/zmq-heartbeat/peer"
	"github.com/sirupsen/logrus"
)

const (
	defaultFile = "updaterd.conf"
)

var (
	log = logrus.New().WithFields(logrus.Fields{
		"type": "main",
	})
)

func main() {
	logrus.SetOutput(os.Stdout)

	configurationFile := defaultFile

	if len(os.Args) > 1 {
		configurationFile = os.Args[1]
	}

	masterConfiguration, err := getConfiguration(configurationFile)
	if nil != err {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Error("failed to read configuration")
		log.Error("abort...")
		return
	}

	log.Info("starting...")

	log.WithFields(logrus.Fields{
		"mode": masterConfiguration.Chain,
	}).Info("chain mode")

	err = zmqutil.StartAuthentication()
	if nil != err {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Error("zmq.AuthStart error")
	}

	err = peer.Initialise(&masterConfiguration.Peering)
	if nil != err {
		log.WithFields(logrus.Fields{
			"error": err,
		}).Error("peer initialise error")
		return
	}
	defer peer.Finalise()

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	sig := <-ch

	log.WithFields(logrus.Fields{
		"signal": sig,
	}).Info("received signal")
	log.Info("shutting down...")
}
