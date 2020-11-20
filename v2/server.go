package discover

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/omecodes/common/dao/mapping"
	"github.com/omecodes/common/errors"
	"github.com/omecodes/common/netx"
	"github.com/omecodes/common/utils/codec"
	"github.com/omecodes/common/utils/log"
	ome "github.com/omecodes/libome/v2"
	"github.com/omecodes/zebou/v2"
	"net"
	"path/filepath"
	"strings"
	"sync"
)

type ServerConfig struct {
	StoreDir     string
	BindAddress  string
	CertFilename string
	KeyFilename  string
}

type msgServer struct {
	sync.Mutex
	handlers map[string]ome.EventHandler
	listener net.Listener
	hub      *zebou.Hub
	store    mapping.DoubleMap
}

func (s *msgServer) NewClient(ctx context.Context, peer *zebou.PeerInfo) {
	if peer != nil {
		log.Info("new client connected", log.Field("conn_id", peer.ID), log.Field("addr", peer.Address))
	} else {
		log.Info("new client connected")
	}

	c, err := s.store.GetAll()
	if err != nil {
		log.Error("could not load services list from store", log.Err(err))
		return
	}

	for c.HasNext() {
		var info ome.ServiceInfo
		err = c.Next(&info)
		if err != nil {
			log.Error("failed to parse service info", log.Err(err))
			return
		}
		encoded, err := codec.Json.Encode(info)
		if err != nil {
			log.Error("failed to json encode service info", log.Err(err))
			return
		}

		err = zebou.Send(ctx, &zebou.ZeMsg{
			Type:    ome.RegistryEventType_Register.String(),
			Id:      info.Id,
			Encoded: encoded,
		})
		if err != nil {
			log.Error("could not send message", log.Err(err))
			return
		}
	}
}

func (s *msgServer) ClientQuit(ctx context.Context, peer *zebou.PeerInfo) {
	log.Info("client disconnected", log.Field("conn_id", peer.ID), log.Field("addr", peer.Address))
	services, err := s.getFromClient(peer.ID)
	if err != nil {
		log.Error("could not get client registered services", log.Err(err))
	}

	err = s.store.DeleteAllMatchingFirstKey(peer.ID)
	if err != nil {
		log.Error("could not delete client registered services", log.Err(err))
		return
	}

	for _, info := range services {
		encoded, err := codec.Json.Encode(info)
		if err != nil {
			log.Error("failed to encode service info", log.Err(err))
			return
		}

		s.hub.Broadcast(ctx, &zebou.ZeMsg{
			Type:    ome.RegistryEventType_DeRegister.String(),
			Id:      info.Id,
			Encoded: encoded,
		})
	}
}

func (s *msgServer) getFromClient(id string) ([]*ome.ServiceInfo, error) {
	c, err := s.store.GetForFirst(id)
	if err != nil {
		log.Error("failed to get registered nodes", log.Field("conn_id", id))
		return nil, err
	}
	defer c.Close()

	var result []*ome.ServiceInfo
	for c.HasNext() {
		var info ome.ServiceInfo
		_, err := c.Next(&info)
		if err != nil {
			log.Error("failed to parse service info", log.Err(err))
			return nil, err
		}
		result = append(result, &info)
	}
	return result, nil
}

func (s *msgServer) OnMessage(ctx context.Context, msg *zebou.ZeMsg) {
	peer := zebou.Peer(ctx)
	go s.hub.Broadcast(ctx, msg)

	switch msg.Type {
	case ome.RegistryEventType_Update.String(), ome.RegistryEventType_Register.String():
		info := new(ome.ServiceInfo)
		err := codec.Json.Decode(msg.Encoded, info)
		if err != nil {
			log.Error("failed to decode service info from message payload", log.Err(err))
			return
		}

		err = s.store.Set(peer.ID, msg.Id, info)
		if err != nil {
			log.Error("failed to store service info", log.Err(err))
			return
		}

		log.Info(msg.Type, log.Field("service", info.Id))

		event := &ome.RegistryEvent{
			ServiceId: msg.Id,
			Info:      info,
		}
		event.Type = ome.RegistryEventType(ome.RegistryEventType_value[msg.Type])
		s.notifyEvent(event)

	case ome.RegistryEventType_DeRegister.String():
		err := s.store.Delete(peer.ID, msg.Id)
		if err != nil {
			log.Error("could not delete service info", log.Err(err), log.Field("service", msg.Id))
			return
		}

		log.Info(msg.Type, log.Field("service", msg.Id))
		s.notifyEvent(&ome.RegistryEvent{
			Type:      ome.RegistryEventType_DeRegister,
			ServiceId: msg.Id,
		})

	case ome.RegistryEventType_DeRegisterNode.String():
		var info ome.ServiceInfo

		err := s.store.Get(peer.ID, msg.Id, &info)
		if err != nil {
			log.Error("failed to read service info", log.Err(err), log.Field("service", msg.Id))
			return
		}

		nodeId := string(msg.Encoded)
		var newNodes []*ome.Node
		for _, node := range info.Nodes {
			if node.Id != nodeId {
				newNodes = append(newNodes, node)
			}
		}

		info.Nodes = newNodes
		err = s.store.Set(peer.ID, msg.Id, info)
		if err != nil {
			log.Error("failed to update service info", log.Err(err), log.Field("service", msg.Id))
			return
		}

		log.Info(msg.Type, log.Field("nodes", string(msg.Encoded)))

		s.notifyEvent(&ome.RegistryEvent{
			Type:      ome.RegistryEventType_DeRegisterNode,
			ServiceId: msg.Id,
		})

	default:
		log.Info("received unsupported msg type", log.Field("type", msg.Type))
	}
}

func (s *msgServer) RegisterService(i *ome.ServiceInfo) error {
	err := s.store.Set("ome", i.Id, i)
	if err != nil {
		return err
	}

	encoded, err := codec.Json.Encode(i)
	if err != nil {
		return err
	}
	msg := &zebou.ZeMsg{
		Type:    ome.RegistryEventType_Register.String(),
		Id:      i.Id,
		Encoded: encoded,
	}
	s.hub.Broadcast(context.Background(), msg)

	s.notifyEvent(&ome.RegistryEvent{
		Type:      ome.RegistryEventType_Register,
		ServiceId: i.Id,
		Info:      i,
	})
	return nil
}

func (s *msgServer) DeregisterService(id string, nodes ...string) error {
	msg := &zebou.ZeMsg{
		Id: id,
	}

	if len(nodes) > 0 {
		var info ome.ServiceInfo
		err := s.store.Get("ome", id, &info)
		if err != nil {
			return err
		}

		var newNodes []*ome.Node
		for _, node := range info.Nodes {
			deleted := true
			for _, nodeId := range nodes {
				if nodeId == node.Id {
					deleted = false
					break
				}
			}

			if !deleted {
				newNodes = append(newNodes, node)
			}
		}
		info.Nodes = newNodes
		err = s.store.Set("ome", msg.Id, info)
		if err != nil {
			return err
		}

		encoded := []byte(strings.Join(nodes, "|"))
		msg.Encoded = encoded
		msg.Type = ome.RegistryEventType_DeRegisterNode.String()
		s.hub.Broadcast(context.Background(), msg)
		ev := &ome.RegistryEvent{
			Type:      ome.RegistryEventType_DeRegisterNode,
			ServiceId: fmt.Sprintf("%s:%s", id, encoded),
		}
		s.notifyEvent(ev)

	} else {
		err := s.store.Delete("ome", id)
		if err != nil {
			return err
		}

		msg.Type = ome.RegistryEventType_DeRegister.String()
		s.hub.Broadcast(context.Background(), msg)
		ev := &ome.RegistryEvent{
			Type:      ome.RegistryEventType_DeRegister,
			ServiceId: id,
		}
		s.notifyEvent(ev)
	}
	return nil
}

func (s *msgServer) GetService(id string) (*ome.ServiceInfo, error) {
	var info ome.ServiceInfo
	err := s.store.Get("ome", id, &info)
	return &info, err
}

func (s *msgServer) GetNode(id string, nodeName string) (*ome.Node, error) {
	info, err := s.GetService(id)
	if err != nil {
		return nil, err
	}

	for _, node := range info.Nodes {
		if node.Id == nodeName {
			return node, nil
		}
	}

	return nil, errors.NotFound
}

func (s *msgServer) Certificate(id string) ([]byte, error) {
	info, err := s.GetService(id)
	if err != nil {
		return nil, err
	}

	strCert, found := info.Meta["certificate"]
	if !found {
		return nil, errors.NotFound
	}
	return []byte(strCert), nil
}

func (s *msgServer) ConnectionInfo(id string, protocol ome.Protocol) (*ome.ConnectionInfo, error) {
	info, err := s.GetService(id)
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

func (s *msgServer) RegisterEventHandler(h ome.EventHandler) string {
	s.Lock()
	defer s.Unlock()
	hid := uuid.New().String()
	s.handlers[hid] = h
	return hid
}

func (s *msgServer) DeregisterEventHandler(hid string) {
	s.Lock()
	defer s.Unlock()
	delete(s.handlers, hid)
}

func (s *msgServer) GetOfType(t ome.ServiceType) ([]*ome.ServiceInfo, error) {
	var msgList []*ome.ServiceInfo
	c, err := s.store.GetAll()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	for c.HasNext() {
		var info ome.ServiceInfo
		err := c.Next(&info)
		if err != nil {
			return msgList, err
		}

		if info.Type == t {
			msgList = append(msgList, nil)
		}
	}
	return msgList, nil
}

func (s *msgServer) FirstOfType(t ome.ServiceType) (*ome.ServiceInfo, error) {
	c, err := s.store.GetAll()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	for c.HasNext() {
		var info ome.ServiceInfo
		err := c.Next(&info)
		if err != nil {
			return nil, err
		}

		if info.Type == t {
			return &info, nil
		}
	}
	return nil, errors.NotFound
}

func (s *msgServer) Stop() error {
	_ = s.hub.Stop()
	return s.listener.Close()
}

func (s *msgServer) notifyEvent(e *ome.RegistryEvent) {
	s.Lock()
	defer s.Unlock()

	for _, h := range s.handlers {
		go h.Handle(e)
	}
}

func Serve(configs *ServerConfig) (ome.Registry, error) {
	s := new(msgServer)
	var opts []netx.ListenOption

	if configs.CertFilename != "" {
		opts = append(opts, netx.Secure(configs.CertFilename, configs.KeyFilename))
	}

	var err error
	s.listener, err = netx.Listen(configs.BindAddress, opts...)
	if err != nil {
		return nil, err
	}

	log.Info("[discovery] starting gRPC server", log.Field("at", s.listener.Addr()))

	db, err := sql.Open("sqlite3", filepath.Join(configs.StoreDir, "registry.db"))
	if err != nil {
		log.Error("could not open registry database", log.Err(err))
		return nil, err
	}

	s.store, err = mapping.NewSQL("sqlite3", db, "reg", codec.Json)
	if err != nil {
		return nil, err
	}

	err = s.store.Clear()
	if err != nil {
		log.Error("failed to reset registry store", log.Err(err))
		return nil, err
	}

	s.hub, err = zebou.Serve(s.listener, s)
	if err != nil {
		return nil, err
	}

	s.handlers = map[string]ome.EventHandler{}

	return s, nil
}
