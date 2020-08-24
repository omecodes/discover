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
	pb2 "github.com/omecodes/libome/proto/service"
	"github.com/omecodes/zebou"
	pb "github.com/omecodes/zebou/proto"
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
	handlers map[string]pb2.EventHandler
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
		var info pb2.Info
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

		err = zebou.Send(ctx, &pb.SyncMessage{
			Type:    pb2.EventType_Register.String(),
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

		s.hub.Broadcast(ctx, &pb.SyncMessage{
			Type:    pb2.EventType_DeRegister.String(),
			Id:      info.Id,
			Encoded: encoded,
		})
	}
}

func (s *msgServer) getFromClient(id string) ([]*pb2.Info, error) {
	c, err := s.store.GetForFirst(id)
	if err != nil {
		log.Error("failed to get registered nodes", log.Field("conn_id", id))
		return nil, err
	}
	defer c.Close()

	var result []*pb2.Info
	for c.HasNext() {
		var info pb2.Info
		_, err := c.Next(&info)
		if err != nil {
			log.Error("failed to parse service info", log.Err(err))
			return nil, err
		}
		result = append(result, &info)
	}
	return result, nil
}

func (s *msgServer) OnMessage(ctx context.Context, msg *pb.SyncMessage) {
	peer := zebou.Peer(ctx)
	go s.hub.Broadcast(ctx, msg)

	switch msg.Type {
	case pb2.EventType_Update.String(), pb2.EventType_Register.String():
		info := new(pb2.Info)
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

		event := &pb2.Event{
			ServiceId: msg.Id,
			Info:      info,
		}
		event.Type = pb2.EventType(pb2.EventType_value[msg.Type])
		s.notifyEvent(event)

	case pb2.EventType_DeRegister.String():
		err := s.store.Delete(peer.ID, msg.Id)
		if err != nil {
			log.Error("could not delete service info", log.Err(err), log.Field("service", msg.Id))
			return
		}

		log.Info(msg.Type, log.Field("service", msg.Id))
		s.notifyEvent(&pb2.Event{
			Type:      pb2.EventType_DeRegister,
			ServiceId: msg.Id,
		})

	case pb2.EventType_DeRegisterNode.String():
		var info pb2.Info

		err := s.store.Get(peer.ID, msg.Id, &info)
		if err != nil {
			log.Error("failed to read service info", log.Err(err), log.Field("service", msg.Id))
			return
		}

		nodeId := string(msg.Encoded)
		var newNodes []*pb2.Node
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

		s.notifyEvent(&pb2.Event{
			Type:      pb2.EventType_DeRegisterNode,
			ServiceId: msg.Id,
		})

	default:
		log.Info("received unsupported msg type", log.Field("type", msg.Type))
	}
}

func (s *msgServer) RegisterService(i *pb2.Info) error {
	err := s.store.Set("ome", i.Id, i)
	if err != nil {
		return err
	}

	encoded, err := codec.Json.Encode(i)
	if err != nil {
		return err
	}
	msg := &pb.SyncMessage{
		Type:    pb2.EventType_Register.String(),
		Id:      i.Id,
		Encoded: encoded,
	}
	s.hub.Broadcast(context.Background(), msg)

	go s.notifyEvent(&pb2.Event{
		Type:      pb2.EventType_Register,
		ServiceId: i.Id,
		Info:      i,
	})
	return nil
}

func (s *msgServer) DeregisterService(id string, nodes ...string) error {
	msg := &pb.SyncMessage{
		Id: id,
	}

	if len(nodes) > 0 {
		var info pb2.Info
		err := s.store.Get("ome", id, &info)
		if err != nil {
			return err
		}

		var newNodes []*pb2.Node
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
		msg.Type = pb2.EventType_DeRegisterNode.String()
		s.hub.Broadcast(context.Background(), msg)
		ev := &pb2.Event{
			Type:      pb2.EventType_DeRegisterNode,
			ServiceId: fmt.Sprintf("%s:%s", id, encoded),
		}
		go s.notifyEvent(ev)

	} else {
		err := s.store.Delete("ome", id)
		if err != nil {
			return err
		}

		msg.Type = pb2.EventType_DeRegister.String()
		s.hub.Broadcast(context.Background(), msg)
		ev := &pb2.Event{
			Type:      pb2.EventType_DeRegister,
			ServiceId: id,
		}
		go s.notifyEvent(ev)
	}
	return nil
}

func (s *msgServer) GetService(id string) (*pb2.Info, error) {
	var info pb2.Info
	err := s.store.Get("local", id, &info)
	return &info, err
}

func (s *msgServer) GetNode(id string, nodeName string) (*pb2.Node, error) {
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

	strCert, found := info.Meta[pb2.MetaServiceCertificate]
	if !found {
		return nil, errors.NotFound
	}
	return []byte(strCert), nil
}

func (s *msgServer) ConnectionInfo(id string, protocol pb2.Protocol) (*pb2.ConnectionInfo, error) {
	info, err := s.GetService(id)
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

func (s *msgServer) RegisterEventHandler(h pb2.EventHandler) string {
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

func (s *msgServer) GetOfType(t pb2.Type) ([]*pb2.Info, error) {
	var msgList []*pb2.Info
	c, err := s.store.GetAll()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	for c.HasNext() {
		var info pb2.Info
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

func (s *msgServer) FirstOfType(t pb2.Type) (*pb2.Info, error) {
	c, err := s.store.GetAll()
	if err != nil {
		return nil, err
	}
	defer c.Close()

	for c.HasNext() {
		var info pb2.Info
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

func (s *msgServer) notifyEvent(e *pb2.Event) {
	s.Lock()
	defer s.Unlock()

	for _, h := range s.handlers {
		h.Handle(e)
	}
}

func Serve(configs *ServerConfig) (pb2.Registry, error) {
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

	s.handlers = map[string]pb2.EventHandler{}

	return s, nil
}
