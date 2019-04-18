package peer

import (
	"bytes"
	"encoding/hex"
	"time"

	"github.com/bitmark-inc/bitmarkd/fault"
	"github.com/bitmark-inc/bitmarkd/mode"
	"github.com/bitmark-inc/bitmarkd/util"
	"github.com/bitmark-inc/bitmarkd/zmqutil"
	"github.com/sirupsen/logrus"

	zmq "github.com/pebbe/zmq4"
)

const (
	subscriberSignal = "inproc://test-subscriber-signal"

	heartbeatInterval = 60 * time.Second
	heartbeatTimeout  = 2 * heartbeatInterval
)

type Subscriber interface {
	Initialise(privateKey []byte, publicKey []byte, connections []Connection) error
	Run(shutdown <-chan struct{})
}

type subscriberData struct {
	log     *logrus.Entry
	push    *zmq.Socket
	pull    *zmq.Socket
	clients []*zmqutil.Client
}

func NewSubscriber(log *logrus.Entry) Subscriber {
	return &subscriberData{
		log: log,
	}
}

// initialise the subscriber
func (sbsc *subscriberData) Initialise(privateKey []byte, publicKey []byte, connections []Connection) error {

	sbsc.log.Info("initialising")

	// validate connection count
	connectionCount := len(connections)
	if 0 == connectionCount {
		sbsc.log.Error("zero connections are available")
		return fault.ErrNoConnectionsAvailable
	}

	// signalling channel
	err := error(nil)
	sbsc.push, sbsc.pull, err = zmqutil.NewSignalPair(subscriberSignal)
	if nil != err {
		return err
	}

	// all sockets
	sbsc.clients = make([]*zmqutil.Client, connectionCount)

	// error for goto fail
	errX := error(nil)

	// connect all static sockets
	for i, c := range connections {
		address, err := util.NewConnection(c.Subscribe)
		if nil != err {
			sbsc.log.WithFields(logrus.Fields{
				"address": c.Subscribe,
				"error":   err,
			}).Error("client connection fail")
			errX = err
			goto fail
		}
		serverPublicKey, err := hex.DecodeString(c.PublicKey)
		if nil != err {
			sbsc.log.WithFields(logrus.Fields{
				"public key": c.PublicKey,
				"error":      err,
			}).Error("decode connection fail")
			errX = err
			goto fail
		}

		// prevent connection to self
		if bytes.Equal(publicKey, serverPublicKey) {
			errX = fault.ErrConnectingToSelfForbidden
			sbsc.log.WithFields(logrus.Fields{
				"public key": c.PublicKey,
				"error":      err,
			}).Error("connec to self")
			goto fail
		}

		client, err := zmqutil.NewClient(zmq.SUB, privateKey, publicKey, 0)
		if nil != err {
			sbsc.log.WithFields(logrus.Fields{
				"client": c.Subscribe,
				"error":  err,
			}).Error("create client fail")
			errX = err
			goto fail
		}

		sbsc.clients[i] = client

		err = client.Connect(address, serverPublicKey, mode.ChainName())
		if nil != err {
			sbsc.log.WithFields(logrus.Fields{
				"client": c.Subscribe,
				"error":  err,
			}).Error("connec fail")
			errX = err
			goto fail
		}
		sbsc.log.Infof("public key: %x  at: %q", serverPublicKey, c.Subscribe)
	}

	return nil

	// error handling
fail:
	zmqutil.CloseClients(sbsc.clients)
	return errX
}

// subscriber main loop
func (sbsc *subscriberData) Run(shutdown <-chan struct{}) {

	sbsc.log.Info("start to run")

	go func() {

		expiryRegister := make(map[*zmq.Socket]time.Time)
		checkAt := time.Now().Add(heartbeatTimeout)
		poller := zmqutil.NewPoller()

		for _, client := range sbsc.clients {
			socket := client.BeginPolling(poller, zmq.POLLIN)
			if nil != socket {
				expiryRegister[socket] = checkAt
			}
		}
		poller.Add(sbsc.pull, zmq.POLLIN)

	loop:
		for {
			sbsc.log.Info("waiting")

			//polled, _ := poller.Poll(-1)
			polled, _ := poller.Poll(heartbeatTimeout)

			now := time.Now()
			expiresAt := now.Add(heartbeatTimeout)
			if now.After(checkAt) {
				logrus.WithFields(logrus.Fields{
					"previous time": heartbeatTimeout.String(),
					"current time":  now.String(),
					"next check":    checkAt.String(),
					"type":          "subscriber",
				}).Warn("timeout exceed")

				checkAt = expiresAt
				for s, expires := range expiryRegister {
					if now.After(expires) {
						client := zmqutil.ClientFromSocket(s)
						logrus.WithFields(logrus.Fields{
							"socket": s.String(),
							"type":   "subscriber",
						}).Error("socket expired")
						if nil == client { // this socket has been closed
							logrus.WithFields(logrus.Fields{
								"socket": s.String,
								"type":   "subscriber",
							}).Error("cannot find client from list")
							sbsc.log.Info("expiry list:")
							for k, v := range expiryRegister {
								logrus.WithFields(logrus.Fields{
									"socket":      k.String,
									"expire time": v.String(),
									"type":        "subscriber",
								}).Info("expiry info")
							}
							delete(expiryRegister, s)
						} else if client.IsConnected() {
							sbsc.log.Infof("client %s reconnect to remote", client.BasicInfo())
							logrus.WithFields(logrus.Fields{
								"client": client.BasicInfo(),
								"type":   "subscriber",
							}).Info("reconnect to remote")
							err := client.ReconnectOpenedSocket()
							if nil != err {
								logrus.WithFields(logrus.Fields{
									"client": client.BasicInfo(),
									"error":  err,
									"type":   "subscriber",
								}).Error("reconnect with error")

							} else {
								delete(expiryRegister, s)
								logrus.WithFields(logrus.Fields{
									"client": client.BasicInfo(),
									"type":   "subscriber",
								}).Error("client try to reconnect, extends expiry time")
								expiryRegister[s] = expiresAt
							}
						} else {
							logrus.WithFields(logrus.Fields{
								"client": client.BasicInfo(),
								"type":   "subscriber",
							}).Error("client try to reconnect, extends expiry time")
							expiryRegister[s] = expiresAt
						}
					} else if expires.Before(checkAt) {
						logrus.WithFields(logrus.Fields{
							"original check tim": checkAt.String(),
							"new check tim":      expires.String(),
							"type":               "subscriber",
						}).Info("shorten check time")

						checkAt = expires
					}
				}
				sbsc.log.Info("finish timeout processing")
			}

			for _, p := range polled {
				switch s := p.Socket; s {
				case sbsc.pull:
					sbsc.log.Info("checking terminate signal")
					_, err := s.RecvMessageBytes(0)
					sbsc.log.Info("receive terminate signal")
					if nil != err {
						logrus.WithFields(logrus.Fields{
							"error": err,
							"type":  "subscriber",
						}).Error("pull receiver error")
					}
					sbsc.log.Info("finish checking terminate signal")
					break loop

				default:
					logrus.WithFields(logrus.Fields{
						"socket": s.String(),
					}).Info("socket info")
					sbsc.log.Info("checking bitmarkd poller socket")
					data, err := s.RecvMessageBytes(0)
					sbsc.log.Info("receive message from socket")
					if nil != err {
						logrus.WithFields(logrus.Fields{
							"error": err,
							"type":  "subscriber",
						}).Error("receive error")
					} else {
						client := zmqutil.ClientFromSocket(s)
						sbsc.log.Info("process received data")
						sbsc.process(data[1:], client)
						sbsc.log.Info("finish process incoming data")
					}
					expiryRegister[s] = expiresAt
				}
			}
			sbsc.log.Info("finish checking poller")
		}
		sbsc.pull.Close()
		zmqutil.CloseClients(sbsc.clients)
	}()

loop:
	for {
		sbsc.log.Info("select…")

		select {
		// wait for shutdown
		case <-shutdown:
			break loop
			// wait for message
		}
	}

	sbsc.push.SendMessage("stop")
	sbsc.push.Close()
}

// process the received subscription
func (sbsc *subscriberData) process(data [][]byte, client *zmqutil.Client) {

	logrus.WithFields(logrus.Fields{
		"client": client.BasicInfo(),
		"type":   "subscriber",
	}).Info("incoming message")

	switch string(data[0]) {
	case "block":
		logrus.WithFields(logrus.Fields{
			"block": data[1],
			"type":  "subscriber",
		}).Info("receive block")
	case "assets":
		logrus.WithFields(logrus.Fields{
			"type":  "subscriber",
			"asset": data[1],
		}).Info("receive asset")
	case "issues":
		logrus.WithFields(logrus.Fields{
			"issue": data[1],
			"type":  "subscriber",
		}).Info("receive issue")
	case "transfer":
		logrus.WithFields(logrus.Fields{
			"transfer": data[1],
			"type":     "subscriber",
		}).Info("receive transfer")
	case "proof":
		logrus.WithFields(logrus.Fields{
			"proof": data[1],
			"type":  "subscriber",
		}).Info("receive proof")
	case "pay":
		logrus.WithFields(logrus.Fields{
			"pay":  data[1],
			"type": "subscriber",
		}).Info("receive pay")
	case "rpc":
		logrus.WithFields(logrus.Fields{
			"fingerprint": data[1],
			"rpc":         data[2],
			"type":        "subscriber",
		}).Info("receive rpc")
	case "peer":
		logrus.WithFields(logrus.Fields{
			"peer":      data[1],
			"broadcast": data[2],
			"listener":  data[3],
			"type":      "subscriber",
		}).Info("receive peer")
	case "heart":
		logrus.WithFields(logrus.Fields{
			"herat": data[1],
			"type":  "subscriber",
		}).Info("receive heart")
	default:
		logrus.WithFields(logrus.Fields{
			"data": data,
			"type": "subscriber",
		}).Warn("receive unhandled")
	}
}
