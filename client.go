package discover

import (
	"crypto/tls"
	"github.com/google/uuid"
	"github.com/omecodes/common/errors"
	"github.com/omecodes/common/utils/codec"
	"github.com/omecodes/common/utils/log"
	ome "github.com/omecodes/libome"
	pb2 "github.com/omecodes/libome/proto/service"
	"github.com/omecodes/zebou"
	pb "github.com/omecodes/zebou/proto"
	"strings"
	"sync"
)

type msgClient struct {
	sync.Mutex
	handlers  map[string]pb2.EventHandler
	messenger *zebou.Client
	store     *sync.Map
}

func (m *msgClient) RegisterService(info *pb2.Info) error {
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

func (m *msgClient) DeregisterService(id string, nodes ...string) error {
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

func (m *msgClient) GetService(id string) (*pb2.Info, error) {
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

func (m *msgClient) GetNode(id string, nodeId string) (*pb2.Node, error) {
	info, err := m.GetService(id)
	if err != nil {
		return nil, err
	}

	for _, n := range info.Nodes {
		if n.Id == nodeId {
			return n, nil
		}
	}
	return nil, errors.NotFound
}

func (m *msgClient) Certificate(id string) ([]byte, error) {
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

func (m *msgClient) ConnectionInfo(id string, protocol pb2.Protocol) (*pb2.ConnectionInfo, error) {
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

func (m *msgClient) RegisterEventHandler(h pb2.EventHandler) string {
	m.Lock()
	defer m.Unlock()
	hid := uuid.New().String()
	m.handlers[hid] = h
	return hid
}

func (m *msgClient) DeregisterEventHandler(id string) {
	m.Lock()
	defer m.Unlock()
	delete(m.handlers, id)
}

func (m *msgClient) GetOfType(t pb2.Type) ([]*pb2.Info, error) {
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

func (m *msgClient) FirstOfType(t pb2.Type) (*pb2.Info, error) {
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

func (m *msgClient) Stop() error {
	return nil
}

func (m *msgClient) handleInbound() {
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

func (m *msgClient) notifyEvent(e *pb2.Event) {
	m.Lock()
	defer m.Unlock()

	for _, h := range m.handlers {
		go h.Handle(e)
	}
}

func NewMSGClient(server string, tlsConfig *tls.Config) *msgClient {
	c := new(msgClient)
	c.store = new(sync.Map)
	c.handlers = map[string]pb2.EventHandler{}

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
