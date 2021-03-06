package discover

import (
	"crypto/tls"
	"encoding/json"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/omecodes/common/errors"
	"github.com/omecodes/common/utils/log"
	"github.com/omecodes/libome"
	"github.com/omecodes/zebou"
)

type ConnectionStateChangesHandler interface {
	HandleConnectionState(connected bool)
}

type HandleConnectionStateFunc func(bool)

func (f HandleConnectionStateFunc) HandleConnectionState(connected bool) {
	f(connected)
}

// MsgClient is a zebou messaging based client client
type MsgClient struct {
	messenger *zebou.Client
	store     *sync.Map
	handlers  *sync.Map

	connectionStateHandleMutex sync.Mutex
	connectionChangesHandlers  map[string]ConnectionStateChangesHandler

	bufferMutex    sync.Mutex
	messagesBuffer []*zebou.ZeMsg
}

// RegisterService sends register message to the discovery server
func (m *MsgClient) RegisterService(info *ome.ServiceInfo) error {

	m.store.Store(info.Id, info)

	encoded, err := json.Marshal(info)
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
		return err
	}

	m.notifyEvent(&ome.RegistryEvent{
		Type:      ome.RegistryEventType_Register,
		ServiceId: info.Id,
		Info:      info,
	})

	log.Error("Registry • registered", log.Field("id", info.Id))
	return nil
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
		return err
	}

	if len(nodes) > 0 {
		log.Error("Registry • registered nodes", log.Field("id", id), log.Field("nodes", nodes))
	} else {
		log.Error("Registry • registered", log.Field("id", id))
	}
	return nil
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
func (m *MsgClient) GetOfType(t uint32) ([]*ome.ServiceInfo, error) {
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
func (m *MsgClient) FirstOfType(t uint32) (*ome.ServiceInfo, error) {
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

		log.Info("registry • received event", log.Field("type", msg.Type))

		switch msg.Type {
		case ome.RegistryEventType_Update.String(), ome.RegistryEventType_Register.String():
			info := new(ome.ServiceInfo)
			err := json.Unmarshal(msg.Encoded, info)
			if err != nil {
				log.Error("failed to decode service info from message payload", log.Err(err))
				return
			}

			log.Info("registry • register service event", log.Field("id", info.Id))
			m.store.Store(info.Id, info)

			event := &ome.RegistryEvent{
				ServiceId: info.Id,
				Info:      info,
			}
			event.Type = ome.RegistryEventType(ome.RegistryEventType_value[msg.Type])
			m.notifyEvent(event)

		case ome.RegistryEventType_DeRegister.String():
			log.Info("registry • delete service event", log.Field("id", msg.Id))
			m.store.Delete(msg.Id)
			m.notifyEvent(&ome.RegistryEvent{
				Type:      ome.RegistryEventType_DeRegister,
				ServiceId: msg.Id,
			})

		case ome.RegistryEventType_DeRegisterNode.String():
			o, ok := m.store.Load(msg.Id)
			if ok {
				info := o.(*ome.ServiceInfo)
				log.Info("registry • register nodes event", log.Field("for", info.Id))

				nodeId := string(msg.Encoded)
				var newNodes []*ome.Node
				for _, node := range info.Nodes {
					if node.Id != nodeId {
						log.Info("registry • new node", log.Field("node", nodeId))
						newNodes = append(newNodes, node)
					}
				}

				info.Nodes = newNodes
				m.store.Store(info.Id, info)

				m.notifyEvent(&ome.RegistryEvent{
					Type:      ome.RegistryEventType_DeRegisterNode,
					ServiceId: info.Id,
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

func (m *MsgClient) notifyConnectionStateChanged(connected bool) {
	m.connectionStateHandleMutex.Lock()
	defer m.connectionStateHandleMutex.Unlock()
	for _, handler := range m.connectionChangesHandlers {
		go handler.HandleConnectionState(connected)
	}
}

func (m *MsgClient) bufferMessages(msg ...*zebou.ZeMsg) {
	m.bufferMutex.Lock()
	defer m.bufferMutex.Unlock()
	m.messagesBuffer = append(m.messagesBuffer, msg...)
}

func (m *MsgClient) sendBufferedMessages() {
	m.bufferMutex.Lock()
	defer m.bufferMutex.Unlock()
	for _, msg := range m.messagesBuffer {
		err := m.messenger.SendMsg(msg)
		if err != nil {
			log.Error("")
		}
	}
}

// NewZebouClient creates and initialize a zebou based registry client
func NewZebouClient(server string, tlsConfig *tls.Config) *MsgClient {
	c := new(MsgClient)
	c.store = new(sync.Map)
	c.handlers = new(sync.Map)

	c.connectionChangesHandlers = map[string]ConnectionStateChangesHandler{}

	c.messenger = zebou.NewClient(server, tlsConfig)
	c.messenger.SetConnectionSateHandler(zebou.ConnectionStateHandlerFunc(func(active bool) {
		if active {
			go c.handleInbound()
			c.store.Range(func(key, value interface{}) bool {
				i := value.(*ome.ServiceInfo)
				err := c.messenger.Send(
					ome.RegistryEventType_Register.String(),
					i.Id,
					i,
				)
				if err != nil {
					log.Error("Registry • failed to send message", log.Err(err))
					return false
				}

				log.Error("Registry • registered", log.Field("id", i.Id))

				c.notifyEvent(&ome.RegistryEvent{
					Type:      ome.RegistryEventType_Register,
					ServiceId: i.Id,
					Info:      i,
				})
				return true
			})
		}
	}))
	c.messenger.Connect()
	return c
}
