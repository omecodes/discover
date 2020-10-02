package discover

import (
	"crypto/tls"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/omecodes/common/errors"
	"github.com/omecodes/common/utils/codec"
	"github.com/omecodes/common/utils/log"
	ome "github.com/omecodes/libome"
	pb2 "github.com/omecodes/libome/proto/service"
	"github.com/omecodes/zebou"
	pb "github.com/omecodes/zebou/proto"
)

// MsgClient is a zebou messaging based client client
type MsgClient struct {
	//handlers  map[string]pb2.EventHandler
	messenger *zebou.Client
	store     *sync.Map
	handlers  *sync.Map
}

// RegisterService sends register message to the discovery server
func (m *MsgClient) RegisterService(info *pb2.Info) error {

	m.store.Store(info.Id, info)
	m.notifyEvent(&pb2.Event{
		Type:      pb2.EventType_Register,
		ServiceId: info.Id,
		Info:      info,
	})

	encoded, err := codec.Json.Encode(info)
	if err != nil {
		log.Info("could not encode service info", log.Err(err))
		return err
	}

	err = m.messenger.SendMsg(&pb.SyncMessage{
		Type:    pb2.EventType_Register.String(),
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
	msg := &pb.SyncMessage{
		Id: id,
	}

	if len(nodes) > 0 {
		encoded = []byte(strings.Join(nodes, "|"))
		msg.Encoded = encoded
		msg.Type = pb2.EventType_DeRegisterNode.String()
	} else {
		msg.Type = pb2.EventType_DeRegister.String()
	}

	err := m.messenger.SendMsg(msg)
	if err != nil {
		log.Error("could not send message to server", log.Err(err))
	}
	return err
}

// GetService returns service info from local store that matches id
func (m *MsgClient) GetService(id string) (*pb2.Info, error) {
	var info *pb2.Info
	m.store.Range(func(key, value interface{}) bool {
		if key == id {
			info = value.(*pb2.Info)
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
func (m *MsgClient) GetNode(id string, nodeID string) (*pb2.Node, error) {
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
func (m *MsgClient) ConnectionInfo(id string, protocol pb2.Protocol) (*pb2.ConnectionInfo, error) {
	info, err := m.GetService(id)
	if err != nil {
		return nil, err
	}

	for _, n := range info.Nodes {
		if protocol == n.Protocol {
			ci := new(pb2.ConnectionInfo)
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
func (m *MsgClient) RegisterEventHandler(h pb2.EventHandler) string {
	hid := uuid.New().String()
	m.handlers.Store(hid, h)
	return hid
}

// DeregisterEventHandler removes the event handler that match id
func (m *MsgClient) DeregisterEventHandler(id string) {
	m.handlers.Delete(id)
}

// GetOfType gets all the service of type t
func (m *MsgClient) GetOfType(t pb2.Type) ([]*pb2.Info, error) {
	var result []*pb2.Info
	m.store.Range(func(key, value interface{}) bool {
		info := value.(*pb2.Info)
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
func (m *MsgClient) FirstOfType(t pb2.Type) (*pb2.Info, error) {
	var info *pb2.Info
	m.store.Range(func(key, value interface{}) bool {
		info = value.(*pb2.Info)
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
		case pb2.EventType_Update.String(), pb2.EventType_Register.String():
			info := new(pb2.Info)
			err := codec.Json.Decode(msg.Encoded, info)
			if err != nil {
				log.Error("failed to decode service info from message payload", log.Err(err))
				return
			}

			m.store.Store(msg.Id, info)

			log.Info(msg.Type, log.Field("service", info.Id))
			event := &pb2.Event{
				ServiceId: msg.Id,
				Info:      info,
			}
			event.Type = pb2.EventType(pb2.EventType_value[msg.Type])
			m.notifyEvent(event)

		case pb2.EventType_DeRegister.String():
			m.store.Delete(msg.Id)
			log.Info(msg.Type, log.Field("service", msg.Id))
			m.notifyEvent(&pb2.Event{
				Type:      pb2.EventType_DeRegister,
				ServiceId: msg.Id,
			})

		case pb2.EventType_DeRegisterNode.String():

			o, ok := m.store.Load(msg.Id)
			if ok {
				info := o.(*pb2.Info)

				nodeId := string(msg.Encoded)
				var newNodes []*pb2.Node
				for _, node := range info.Nodes {
					if node.Id != nodeId {
						newNodes = append(newNodes, node)
					}
				}

				info.Nodes = newNodes
				m.store.Store(msg.Id, info)
				log.Info(msg.Type, log.Field("nodes", string(msg.Encoded)))

				m.notifyEvent(&pb2.Event{
					Type:      pb2.EventType_DeRegisterNode,
					ServiceId: msg.Id,
				})
			}

		default:
			log.Info("received unsupported msg type", log.Field("type", msg.Type))
		}
	}
}

func (m *MsgClient) notifyEvent(e *pb2.Event) {
	m.handlers.Range(func(key, value interface{}) bool {
		h := value.(pb2.EventHandler)
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
	go c.handleInbound()

	return c
}
