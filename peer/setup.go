package peer

import (
	"os"
	"sync"

	"github.com/bitmark-inc/bitmarkd/zmqutil"
	"github.com/sirupsen/logrus"
)

type Connection struct {
	PublicKey string `gluamapper:"public_key" json:"public_key"`
	Subscribe string `gluamapper:"subscribe" json:"subscribe"`
	Connect   string `gluamapper:"connect" json:"connect"`
}

type Configuration struct {
	PrivateKey string       `gluamapper:"private_key" json:"private_key"`
	PublicKey  string       `gluamapper:"public_key" json:"public_key"`
	Node       []Connection `gluamapper:"node" json:"node"`
}

type peerData struct {
	sync.RWMutex // to allow locking

	sbsc subscriber // for subscriptions
}

// global data
var globalData peerData
var log = logrus.New().WithFields(logrus.Fields{
	"type": "subscriber",
})
var shutdown chan struct{}

func Initialise(configuration *Configuration) error {
	logrus.SetOutput(os.Stdout)
	logrus.SetReportCaller(true)
	logrus.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
	})

	globalData.Lock()
	defer globalData.Unlock()

	log.Info("starting…")

	log.WithFields(logrus.Fields{
		"data": configuration,
	}).Info("configuration")

	// read the keys
	privateKey, err := zmqutil.ReadPrivateKey(configuration.PrivateKey)
	if nil != err {
		log.WithFields(logrus.Fields{
			"private key": configuration.PrivateKey,
			"error":       err,
		}).Error("read private key fail")
		return err
	}
	publicKey, err := zmqutil.ReadPublicKey(configuration.PublicKey)
	if nil != err {
		log.WithFields(logrus.Fields{
			"public key": configuration.PublicKey,
			"error":      err,
		}).Error("read public key file")
		return err
	}
	log.WithFields(logrus.Fields{
		"private key": privateKey,
	}).Info("peer private key")
	log.WithFields(logrus.Fields{
		"public key": publicKey,
	}).Info("peer public key")

	if err := globalData.sbsc.initialise(privateKey, publicKey, configuration.Node); nil != err {
		return err
	}

	shutdown = make(chan struct{})

	go func() {
		globalData.sbsc.Run(shutdown)
	}()

	return nil
}

func Finalise() {
	shutdown <- struct{}{}
}
