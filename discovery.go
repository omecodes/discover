package discover

import (
	"github.com/google/uuid"
	"github.com/omecodes/common/codec"
	"github.com/omecodes/common/doer"
	"github.com/omecodes/common/errors"
	"github.com/omecodes/common/log"
	pb2 "github.com/omecodes/common/proto/service"
	pb "github.com/omecodes/zebou/proto"
	"strings"
	"sync"
)

type MessageSender interface {
	SendMsg(msg *pb.SyncMessage) error
}

type registry struct {
	sync.Mutex
	sender   MessageSender
	store    *sync.Map
	stopper  doer.Stopper
	handlers map[string]pb2.EventHandler
}

func (r *registry) RegisterService(i *pb2.Info) error {
	encoded, err := codec.Json.Encode(i)
	if err != nil {
		return err
	}
	msg := &pb.SyncMessage{
		Type:    pb2.EventType_Register.String(),
		Id:      i.Id,
		Encoded: encoded,
	}

	err = r.sender.SendMsg(msg)
	if err == nil {
		go r.Handle(msg)
	}
	return err
}

func (r *registry) DeregisterService(id string, nodes ...string) error {
	msg := &pb.SyncMessage{
		Id: id,
	}
	if len(nodes) > 0 {
		encoded := []byte(strings.Join(nodes, "|"))
		msg.Encoded = encoded
		msg.Type = pb2.EventType_DeRegisterNode.String()

	} else {
		msg.Type = pb2.EventType_DeRegister.String()
	}
	err := r.sender.SendMsg(msg)
	if err == nil {
		r.Handle(msg)
	}
	return err
}

func (r *registry) GetService(id string) (*pb2.Info, error) {
	o, found := r.store.Load(id)
	if !found {
		return nil, errors.NotFound
	}
	return o.(*pb2.Info), nil
}

func (r *registry) GetNode(id string, nodeName string) (*pb2.Node, error) {
	info, err := r.GetService(id)
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

func (r *registry) Certificate(id string) ([]byte, error) {
	info, err := r.GetService(id)
	if err != nil {
		return nil, err
	}

	strCert, found := info.Meta[pb2.MetaServiceCertificate]
	if !found {
		return nil, errors.NotFound
	}
	return []byte(strCert), nil
}

func (r *registry) ConnectionInfo(id string, protocol pb2.Protocol) (*pb2.ConnectionInfo, error) {
	s, err := r.GetService(id)
	if err != nil {
		return nil, err
	}

	for _, n := range s.Nodes {
		if protocol == n.Protocol {
			ci := new(pb2.ConnectionInfo)
			ci.Address = n.Address
			strCert, found := s.Meta["certificate"]
			if !found {
				return ci, nil
			}
			ci.Certificate = []byte(strCert)
			return ci, nil
		}
	}

	return nil, errors.NotFound
}

func (r *registry) RegisterEventHandler(h pb2.EventHandler) string {
	r.Lock()
	defer r.Unlock()
	hid := uuid.New().String()
	r.handlers[hid] = h
	return hid
}

func (r *registry) DeregisterEventHandler(hid string) {
	r.Lock()
	defer r.Unlock()
	delete(r.handlers, hid)
}

func (r *registry) GetOfType(t pb2.Type) ([]*pb2.Info, error) {
	var infos []*pb2.Info
	r.store.Range(func(key, value interface{}) bool {
		i := value.(*pb2.Info)
		if i.Type == t {
			infos = append(infos, i)
		}
		return true
	})
	return infos, nil
}

func (r *registry) FirstOfType(t pb2.Type) (*pb2.Info, error) {
	var infos []*pb2.Info
	r.store.Range(func(key, value interface{}) bool {
		i := value.(*pb2.Info)
		if i.Type == t {
			infos = append(infos, i)
		}
		return true
	})
	if len(infos) > 0 {
		return infos[0], nil
	}
	return nil, errors.NotFound
}

func (r *registry) Stop() error {
	if r.stopper != nil {
		return r.stopper.Stop()
	}
	return nil
}

func (r *registry) Handle(msg *pb.SyncMessage) {
	switch msg.Type {
	case pb2.EventType_Update.String(), pb2.EventType_Register.String():
		info := new(pb2.Info)
		err := codec.Json.Decode(msg.Encoded, info)
		if err != nil {
			log.Error("failed to decode service info", err)
			return
		}
		r.store.Store(msg.Id, info)

		event := &pb2.Event{
			ServiceId: info.Id,
			Info:      info,
		}
		if pb2.EventType_Register.String() == msg.Type {
			event.Type = pb2.EventType_Register
		} else {
			event.Type = pb2.EventType_Update
		}
		r.handleEvent(event)

	case pb2.EventType_DeRegister.String():
		r.store.Delete(msg.Id)
		event := &pb2.Event{
			Type:      pb2.EventType_DeRegister,
			ServiceId: msg.Id,
		}
		r.handleEvent(event)

	case pb2.EventType_DeRegisterNode.String():
		o, ok := r.store.Load(msg.Id)
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
			r.store.Store(msg.Id, info)

			event := &pb2.Event{
				Type:      pb2.EventType_Update,
				ServiceId: msg.Id,
				Info:      info,
			}
			r.handleEvent(event)
		}

	default:
		log.Info("received unsupported msg type", log.Field("type", msg.Type))
		return
	}
}

func (r *registry) handleEvent(event *pb2.Event) {
	r.Lock()
	defer r.Unlock()

	for _, h := range r.handlers {
		go h.Handle(event)
	}
}

func newRegistry(store *sync.Map, sender MessageSender, stopper doer.Stopper) *registry {
	return &registry{
		sender:   sender,
		store:    store,
		stopper:  stopper,
		handlers: map[string]pb2.EventHandler{},
	}
}
