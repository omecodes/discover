package discover

import (
	"crypto/tls"
	"github.com/omecodes/common/utils/log"
	pb2 "github.com/omecodes/libome/proto/service"
	"github.com/omecodes/zebou"
	pb "github.com/omecodes/zebou/proto"
	"sync"
)

type msgClient struct {
	messenger *zebou.Client
	*registry
	store *sync.Map
}

func (m *msgClient) handleMessage(msg *pb.SyncMessage) {
	if m.registry != nil {
		m.registry.Handle(msg)
	}
}

func NewMSGClient(server string, tlsConfig *tls.Config) *msgClient {
	c := new(msgClient)
	c.store = new(sync.Map)

	c.messenger = zebou.Connect(server, tlsConfig, pb.MessageHandlerFunc(c.handleMessage))
	c.registry = newRegistry(c.store, c.messenger, c.messenger)
	c.messenger.SetConnectionSateHandler(zebou.ConnectionStateHandlerFunc(func(active bool) {
		if active {
			c.store.Range(func(key, value interface{}) bool {
				i := value.(*pb2.Info)
				err := c.messenger.Send(
					pb2.EventType_Register.String(),
					i.Id,
					i,
				)

				if err != nil {
					log.Error("failed to send message", log.Err(err))
					return false
				}
				return true
			})
			log.Info("sent all info to server")
		}
	}))
	return c
}
