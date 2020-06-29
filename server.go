package discover

import (
	"github.com/omecodes/common/codec"
	"github.com/omecodes/common/doer"
	"github.com/omecodes/common/errors"
	"github.com/omecodes/common/log"
	"github.com/omecodes/common/netx"
	pb2 "github.com/omecodes/common/proto/service"
	"github.com/omecodes/zebou"
	pb "github.com/omecodes/zebou/proto"
	"net"
	"sync"
)

type ServerConfig struct {
	BindAddress  string
	CertFilename string
	KeyFilename  string
}

type msgServer struct {
	sync.Mutex
	listener      net.Listener
	stopper       doer.Stopper
	store         *sync.Map
	eventHandlers map[string]pb2.EventHandler
}

func (s *msgServer) Handle(msg *pb.SyncMessage) error {
	switch msg.Type {
	case pb2.EventType_Update.String(), pb2.EventType_Register.String():
		info := new(pb2.Info)
		err := codec.Json.Decode(msg.Encoded, info)
		if err != nil {
			return err
		}
		s.store.Store(msg.Id, info)

		event := &pb2.Event{
			ServiceId: msg.Type,
			Info:      info,
		}
		if pb2.EventType_Register.String() == msg.Type {
			event.Type = pb2.EventType_Register
		} else {
			event.Type = pb2.EventType_Update
		}

	case pb2.EventType_DeRegister.String():
		s.store.Delete(msg.Id)

	case pb2.EventType_DeRegisterNode.String():
		o, ok := s.store.Load(msg.Id)
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
			s.store.Store(msg.Id, info)
		}

	default:
		log.Info("received unsupported msg type", log.Field("type", msg.Type))
		return errors.NotSupported
	}
	return nil
}

func (s *msgServer) State() ([]*pb.SyncMessage, error) {
	var msgs []*pb.SyncMessage
	s.store.Range(func(key, value interface{}) bool {
		info := value.(*pb2.Info)
		encoded, _ := codec.Json.Encode(info)
		msgs = append(msgs, &pb.SyncMessage{Id: info.Id, Type: pb2.EventType_Register.String(), Encoded: encoded})
		return true
	})
	return msgs, nil
}

func (s *msgServer) Invalidate(id string) error {
	s.store.Delete(id)
	return nil
}

func (s *msgServer) SendMsg(msg *pb.SyncMessage) error {
	return s.Handle(msg)
}

func (s *msgServer) Stop() error {
	_ = s.stopper.Stop()
	return s.listener.Close()
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

	s.store = &sync.Map{}
	s.eventHandlers = map[string]pb2.EventHandler{}
	s.stopper, err = zebou.Serve(s.listener, s)
	if err != nil {
		return nil, err
	}

	return newRegistry(s.store, s, s), nil
}
