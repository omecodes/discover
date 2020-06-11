package discover

import (
	"crypto/tls"
	"github.com/omecodes/zebou"
	pb "github.com/omecodes/zebou/proto"
	"sync"
)

type msgClient struct {
	messenger *zebou.Client
	*registry
}

func (m *msgClient) handleMessage(msg *pb.SyncMessage) {
	if m.registry != nil {
		m.registry.Handle(msg)
	}
}

func NewMSGClient(server string, tlsConfig *tls.Config) *msgClient {
	c := new(msgClient)
	c.messenger = zebou.Connect(server, tlsConfig, pb.MessageHandlerFunc(c.handleMessage))
	c.registry = newRegistry(&sync.Map{}, c.messenger, c.messenger)
	return c
}
