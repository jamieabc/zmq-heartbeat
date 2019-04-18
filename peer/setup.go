package peer

import (
	"os"
	"sync"

	"github.com/bitmark-inc/bitmarkd/zmqutil"
	"github.com/sirupsen/logrus"
)

type Peer interface {
	Initialise(configuration *Configuration) error
	Finalise()
}

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
	sync.RWMutex

	sbsc     Subscriber
	log      *logrus.Entry
	shutdown chan struct{}
}

func NewPeer() Peer {
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})

	log := logrus.New().WithFields(logrus.Fields{
		"type": "subscriber",
	})

	return &peerData{
		log:      log,
		shutdown: make(chan struct{}),
		sbsc:     NewSubscriber(log),
	}
}

func (p *peerData) Initialise(configuration *Configuration) error {

	p.Lock()
	defer p.Unlock()

	p.log.Info("start")

	// read the keys
	privateKey, err := zmqutil.ReadPrivateKey(configuration.PrivateKey)
	if nil != err {
		p.log.WithFields(logrus.Fields{
			"private key": configuration.PrivateKey,
			"error":       err,
		}).Error("read private key fail")
		return err
	}
	publicKey, err := zmqutil.ReadPublicKey(configuration.PublicKey)
	if nil != err {
		p.log.WithFields(logrus.Fields{
			"public key": configuration.PublicKey,
			"error":      err,
		}).Error("read public key file")
		return err
	}

	if err := p.sbsc.Initialise(privateKey, publicKey, configuration.Node); nil != err {
		return err
	}

	go func() {
		p.sbsc.Run(p.shutdown)
	}()

	return nil
}

func (p *peerData) Finalise() {
	p.log.Info("terminating subscriber")
	p.shutdown <- struct{}{}
}
