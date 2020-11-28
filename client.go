package discover

import (
	"crypto/tls"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/omecodes/common/errors"
	"github.com/omecodes/common/utils/codec"
	"github.com/omecodes/common/utils/log"
	"github.com/omecodes/libome"
	"github.com/omecodes/zebou"
)

// MsgClient is a zebou messaging based client client
type MsgClient struct {
	//handlers  map[string]ome.EventHandler
	messenger *zebou.Client
	store     *sync.Map
	handlers  *sync.Map
}

// RegisterService sends register message to the discovery server
func (m *MsgClient) RegisterService(info *ome.ServiceInfo) error {

	m.store.Store(info.Id, info)
	m.notifyEvent(&ome.RegistryEvent{
		Type:      ome.RegistryEventType_Register,
		ServiceId: info.Id,
		Info:      info,
	})

	encoded, err := codec.Json.Encode(info)
	if err != nil {
		log.Info("could not encode service info", log.Err(err))
		return err
	}

	err = m.messenger.SendMsg(&zebou.ZeMsg{
		Type:    ome.RegistryEventType_Register.String(),
		Id:      info.Id,
		Encoded: encoded,
	})
	if err != nil {
		log.Error("could not send message to server", log.Err(err))
	}
	return err
}

// DeregisterService sends a deregister message to the discovery server
func (m *MsgClient) DeregisterService(id string, nodes ...string) error {
	var encoded []byte
	msg := &zebou.ZeMsg{
		Id: id,
	}

	if len(nodes) > 0 {
		encoded = []byte(strings.Join(nodes, "|"))
		msg.Encoded = encoded
		msg.Type = ome.RegistryEventType_DeRegisterNode.String()
	} else {
		msg.Type = ome.RegistryEventType_DeRegister.String()
	}

	err := m.messenger.SendMsg(msg)
	if err != nil {
		log.Error("could not send message to server", log.Err(err))
	}
	return err
}

// GetService returns service info from local store that matches id
func (m *MsgClient) GetService(id string) (*ome.ServiceInfo, error) {
	var info *ome.ServiceInfo
	m.store.Range(func(key, value interface{}) bool {
		if key == id {
			info = value.(*ome.ServiceInfo)
			return false
		}
		return true
	})
	if info == nil {
		return nil, errors.NotFound
	}
	return info, nil
}

// GetNode returns the gRPC node of the service info from local store that matches id
func (m *MsgClient) GetNode(id string, nodeID string) (*ome.Node, error) {
	info, err := m.GetService(id)
	if err != nil {
		return nil, err
	}

	for _, n := range info.Nodes {
		if n.Id == nodeID {
			return n, nil
		}
	}
	return nil, errors.NotFound
}

// Certificate returns the PEM encoded certificate of the service info from local store that matches id
func (m *MsgClient) Certificate(id string) ([]byte, error) {
	info, err := m.GetService(id)
	if err != nil {
		return nil, err
	}
	strCert, found := info.Meta[ome.MetaServiceCertificate]
	if !found {
		return nil, errors.NotFound
	}
	return []byte(strCert), nil
}

// ConnectionInfo finds the service frolm local store that matches id, and return the connection info of the node that implement the given transport protocol
func (m *MsgClient) ConnectionInfo(id string, protocol ome.Protocol) (*ome.ConnectionInfo, error) {
	info, err := m.GetService(id)
	if err != nil {
		return nil, err
	}

	for _, n := range info.Nodes {
		if protocol == n.Protocol {
			ci := new(ome.ConnectionInfo)
			ci.Address = n.Address
			strCert, found := info.Meta["certificate"]
			if !found {
				return ci, nil
			}
			ci.Certificate = []byte(strCert)
			return ci, nil
		}
	}

	return nil, errors.NotFound
}

// RegisterEventHandler adds an event handler. Returns an id that is used to deregister h
func (m *MsgClient) RegisterEventHandler(h ome.EventHandler) string {
	hid := uuid.New().String()
	m.handlers.Store(hid, h)
	return hid
}

// DeregisterEventHandler removes the event handler that match id
func (m *MsgClient) DeregisterEventHandler(id string) {
	m.handlers.Delete(id)
}

// GetOfType gets all the service of type t
func (m *MsgClient) GetOfType(t ome.ServiceType) ([]*ome.ServiceInfo, error) {
	var result []*ome.ServiceInfo
	m.store.Range(func(key, value interface{}) bool {
		info := value.(*ome.ServiceInfo)
		if info.Type == t {
			result = append(result, info)
		}
		return true
	})
	if len(result) == 0 {
		return nil, errors.NotFound
	}

	return result, nil
}

// FirstOfType returns the first service from local store of type t
func (m *MsgClient) FirstOfType(t ome.ServiceType) (*ome.ServiceInfo, error) {
	var info *ome.ServiceInfo
	m.store.Range(func(key, value interface{}) bool {
		info = value.(*ome.ServiceInfo)
		return info.Type != t
	})
	if info == nil {
		return nil, errors.NotFound
	}
	return info, nil
}

// Stop closes the messaging connection
func (m *MsgClient) Stop() error {
	return nil
}

func (m *MsgClient) handleInbound() {
	for {
		msg, err := m.messenger.GetMessage()
		if err != nil {
			log.Error("failed to get next message", log.Err(err))
			continue
		}

		switch msg.Type {
		case ome.RegistryEventType_Update.String(), ome.RegistryEventType_Register.String():
			info := new(ome.ServiceInfo)
			err := codec.Json.Decode(msg.Encoded, info)
			if err != nil {
				log.Error("failed to decode service info from message payload", log.Err(err))
				return
			}

			m.store.Store(msg.Id, info)

			log.Info(msg.Type, log.Field("service", info.Id))
			event := &ome.RegistryEvent{
				ServiceId: msg.Id,
				Info:      info,
			}
			event.Type = ome.RegistryEventType(ome.RegistryEventType_value[msg.Type])
			m.notifyEvent(event)

		case ome.RegistryEventType_DeRegister.String():
			m.store.Delete(msg.Id)
			log.Info(msg.Type, log.Field("service", msg.Id))
			m.notifyEvent(&ome.RegistryEvent{
				Type:      ome.RegistryEventType_DeRegister,
				ServiceId: msg.Id,
			})

		case ome.RegistryEventType_DeRegisterNode.String():

			o, ok := m.store.Load(msg.Id)
			if ok {
				info := o.(*ome.ServiceInfo)

				nodeId := string(msg.Encoded)
				var newNodes []*ome.Node
				for _, node := range info.Nodes {
					if node.Id != nodeId {
						newNodes = append(newNodes, node)
					}
				}

				info.Nodes = newNodes
				m.store.Store(msg.Id, info)
				log.Info(msg.Type, log.Field("nodes", string(msg.Encoded)))

				m.notifyEvent(&ome.RegistryEvent{
					Type:      ome.RegistryEventType_DeRegisterNode,
					ServiceId: msg.Id,
				})
			}

		default:
			log.Info("received unsupported msg type", log.Field("type", msg.Type))
		}
	}
}

func (m *MsgClient) notifyEvent(e *ome.RegistryEvent) {
	m.handlers.Range(func(key, value interface{}) bool {
		h := value.(ome.EventHandler)
		go h.Handle(e)
		return true
	})
}

// NewMSGClient creates and initialize a discovery client based
func NewMSGClient(server string, tlsConfig *tls.Config) *MsgClient {
	c := new(MsgClient)
	c.store = new(sync.Map)
	c.handlers = new(sync.Map)

	c.messenger = zebou.Connect(server, tlsConfig)
	c.messenger.SetConnectionSateHandler(zebou.ConnectionStateHandlerFunc(func(active bool) {
		if active {
			c.store.Range(func(key, value interface{}) bool {
				i := value.(*ome.ServiceInfo)
				err := c.messenger.Send(
					ome.RegistryEventType_Register.String(),
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
	go c.handleInbound()

	return c
}
