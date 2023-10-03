package dht

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/v2/private_routing"
	record "github.com/libp2p/go-libp2p-record"
	recpb "github.com/libp2p/go-libp2p-record/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"go.opentelemetry.io/otel/attribute"
	otel "go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/slog"

	"github.com/libp2p/go-libp2p-kad-dht/v2/kadt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

// handleFindPeer handles FIND_NODE requests from remote peers.
func (d *DHT) handleFindPeer(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	if len(req.GetKey()) == 0 {
		return nil, fmt.Errorf("handleFindPeer with empty key")
	}

	// tell the coordinator that this peer supports finding closer nodes
	d.kad.NotifyConnectivity(ctx, kadt.PeerID(remote))

	// "parse" requested peer ID from the key field
	target := peer.ID(req.GetKey())

	// initialize the response message
	resp := &pb.Message{
		Type: pb.Message_FIND_NODE,
		Key:  req.GetKey(),
	}

	// get reference to peer store
	pstore := d.host.Peerstore()

	// if the remote is asking for us, short-circuit and return us only
	if target == d.host.ID() {
		resp.CloserPeers = []*pb.Message_Peer{pb.FromAddrInfo(pstore.PeerInfo(d.host.ID()))}
		return resp, nil
	}

	// gather closer peers that we know
	resp.CloserPeers = d.closerPeers(ctx, remote, kadt.PeerID(target).Key())

	// if we happen to know the target peers addresses (e.g., although we are
	// far away in the keyspace), we add the peer to the result set. This means
	// we potentially return bucketSize + 1 peers. We won't add the peer to the
	// response if it's already contained in CloserPeers.
	targetInfo := pstore.PeerInfo(target)
	if len(targetInfo.Addrs) > 0 && !resp.ContainsCloserPeer(target) && target != remote {
		resp.CloserPeers = append(resp.CloserPeers, pb.FromAddrInfo(targetInfo))
	}

	return resp, nil
}

// handlePing handles PING requests from remote peers.
func (d *DHT) handlePing(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	d.log.LogAttrs(ctx, slog.LevelDebug, "Responding to ping", slog.String("remote", remote.String()))
	return &pb.Message{Type: pb.Message_PING}, nil
}

// handleGetValue handles PUT_VALUE RPCs from remote peers.
func (d *DHT) handlePutValue(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	// validate incoming request -> key and record must not be empty/nil
	k := string(req.GetKey())
	if len(k) == 0 {
		return nil, fmt.Errorf("no key was provided")
	}

	rec := req.GetRecord()
	if rec == nil {
		return nil, fmt.Errorf("nil record")
	}

	if !bytes.Equal(req.GetKey(), rec.GetKey()) {
		return nil, fmt.Errorf("key doesn't match record key")
	}

	// TODO: use putValueLocal?

	// key is /$namespace/$binary_id
	ns, path, err := record.SplitKey(k) // get namespace (prefix of the key)
	if err != nil || len(path) == 0 {
		return nil, fmt.Errorf("invalid key %s: %w", k, err)
	}

	backend, found := d.backends[ns]
	if !found {
		return nil, fmt.Errorf("unsupported record type: %s", ns)
	}

	_, err = backend.Store(ctx, path, rec)

	return nil, err
}

// handleGetValue handles GET_VALUE RPCs from remote peers.
func (d *DHT) handleGetValue(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	k := string(req.GetKey())
	if len(k) == 0 {
		return nil, fmt.Errorf("handleGetValue but no key in request")
	}

	// prepare the response message
	resp := &pb.Message{
		Type:        pb.Message_GET_VALUE,
		Key:         req.GetKey(),
		CloserPeers: d.closerPeers(ctx, remote, kadt.NewKey(req.GetKey())),
	}

	ns, path, err := record.SplitKey(k) // get namespace (prefix of the key)
	if err != nil || path == "" {
		return nil, fmt.Errorf("invalid key %s: %w", k, err)
	}

	backend, found := d.backends[ns]
	if !found {
		return nil, fmt.Errorf("unsupported record type: %s", ns)
	}

	fetched, err := backend.Fetch(ctx, path)
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return resp, nil
		}
		return nil, fmt.Errorf("fetch record for key %s: %w", k, err)
	} else if fetched == nil {
		return resp, nil
	}

	rec, ok := fetched.(*recpb.Record)
	if ok {
		resp.Record = rec
		return resp, nil
	}
	// the returned value wasn't a record, which could be the case if the
	// key was prefixed with "providers."

	pset, ok := fetched.(*providerSet)
	if ok {
		resp.ProviderPeers = make([]*pb.Message_Peer, len(pset.providers))
		for i, p := range pset.providers {
			resp.ProviderPeers[i] = pb.FromAddrInfo(p)
		}

		return resp, nil
	}

	return nil, fmt.Errorf("expected *recpb.Record or *providerSet value type, got: %T", pset)
}

// handleAddProvider handles ADD_PROVIDER RPCs from remote peers.
func (d *DHT) handleAddProvider(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	k := string(req.GetKey())
	if len(k) > 80 {
		return nil, fmt.Errorf("key size too large")
	} else if len(k) == 0 {
		return nil, fmt.Errorf("key is empty")
	} else if len(req.GetProviderPeers()) == 0 {
		return nil, fmt.Errorf("no provider peers given")
	}

	var addrInfos []peer.AddrInfo
	for _, addrInfo := range req.ProviderAddrInfos() {
		addrInfo := addrInfo // TODO: remove after go.mod was updated to go 1.21

		if addrInfo.ID != remote {
			return nil, fmt.Errorf("attempted to store provider record for other peer %s", addrInfo.ID)
		}

		if len(addrInfo.Addrs) == 0 {
			return nil, fmt.Errorf("no addresses for provider")
		}

		addrInfos = append(addrInfos, addrInfo)
	}

	backend, ok := d.backends[namespaceProviders]
	if !ok {
		return nil, fmt.Errorf("unsupported record type: %s", namespaceProviders)
	}

	for _, addrInfo := range addrInfos {
		if _, err := backend.Store(ctx, k, addrInfo); err != nil {
			return nil, fmt.Errorf("storing provider record: %w", err)
		}
	}

	return nil, nil
}

// handleGetProviders handles GET_PROVIDERS RPCs from remote peers.
func (d *DHT) handleGetProviders(ctx context.Context, remote peer.ID, req *pb.Message) (*pb.Message, error) {
	k := req.GetKey()
	if len(k) > 80 {
		return nil, fmt.Errorf("handleGetProviders key size too large")
	} else if len(k) == 0 {
		return nil, fmt.Errorf("handleGetProviders key is empty")
	}

	backend, ok := d.backends[namespaceProviders]
	if !ok {
		return nil, fmt.Errorf("unsupported record type: %s", namespaceProviders)
	}

	resp := &pb.Message{
		Type:        pb.Message_GET_PROVIDERS,
		Key:         k,
		CloserPeers: d.closerPeers(ctx, remote, kadt.NewKey(k)),
	}

	fetched, err := backend.Fetch(ctx, string(req.GetKey()))
	if err != nil {
		if errors.Is(err, ds.ErrNotFound) {
			return resp, nil
		}
		return nil, fmt.Errorf("fetch providers from datastore: %w", err)
	}

	pset, ok := fetched.(*providerSet)
	if !ok {
		return nil, fmt.Errorf("expected *providerSet value type, got: %T", pset)
	}

	pbProviders := make([]*pb.Message_Peer, len(pset.providers))
	for i, p := range pset.providers {
		pbProviders[i] = pb.FromAddrInfo(p)
	}

	resp.ProviderPeers = pbProviders

	return resp, nil
}

// closerPeers returns the closest peers to the given target key this host knows
// about. It doesn't return 1) itself 2) the peer that asked for closer peers.
func (d *DHT) closerPeers(ctx context.Context, remote peer.ID, target kadt.Key) []*pb.Message_Peer {
	_, span := d.tele.Tracer.Start(ctx, "DHT.closerPeers", otel.WithAttributes(attribute.String("remote", remote.String()), attribute.String("target", target.HexString())))
	defer span.End()

	peers := d.rt.NearestNodes(target, d.cfg.BucketSize)
	if len(peers) == 0 {
		return nil
	}

	// pre-allocated the result set slice.
	filtered := make([]*pb.Message_Peer, 0, len(peers))
	for _, p := range peers {
		pid := peer.ID(p) // TODO: type cast

		// check for own peer ID
		if pid == d.host.ID() {
			d.log.Warn("routing table NearestNodes returned our own ID")
			continue
		}

		// Don't send a peer back themselves
		if pid == remote {
			continue
		}

		// extract peer information from peer store and only add it to the
		// final list if we know any addresses of that peer.
		addrInfo := d.host.Peerstore().PeerInfo(pid)
		if len(addrInfo.Addrs) == 0 {
			continue
		}

		filtered = append(filtered, pb.FromAddrInfo(addrInfo))
	}

	return filtered
}

// Responds to a PIR request in a private FindNode message with a PIR response.
func (d *DHT) handlePrivateFindPeer(ctx context.Context, remote peer.ID, msg *pb.Message) (*pb.Message, error) {
	_, span := d.tele.Tracer.Start(ctx, "DHT.handlePrivateFindPeer", otel.WithAttributes(attribute.String("remote", remote.String())))
	defer span.End()

	pirRequest := msg.GetEncryptedQuery()
	if pirRequest == nil {
		return nil, fmt.Errorf("no PIR Request sent in the message")
	}

	bucketsWithAddrInfos := d.normalizeRTJoinedWithPeerStore()

	encrypted_peer_ids, err := private_routing.RunPIRforCloserPeersRecords(pirRequest, bucketsWithAddrInfos)
	if err != nil {
		return nil, err
	}

	// TODO Ask Gui: handleFindPeer also looks up peerStore directly for the target key and adds it to the closerPeers.
	// This might be necessary as we may not store the node's (KadID, PeerID) if our bucket is full,
	// but we may still record the addresses of the node in the peer store?
	// So do we need to do another PIR over the peer store?
	// Or before we normalize the RT,
	// can we "fill up" our RT with kadID, peerID of records that are in the peerStore but not in the RT?

	pirResponse := &pb.PIR_Response{
		Id:          pirRequest.Id,
		CloserPeers: encrypted_peer_ids,
	}

	response := &pb.Message{
		Type:             pb.Message_PRIVATE_FIND_NODE,
		EncryptedRecords: pirResponse,
	}

	return response, nil
}

// Responds to a PIR request in a private GetProviders message with a PIR response.
func (d *DHT) handlePrivateGetProviderRecords(ctx context.Context, remote peer.ID, msg *pb.Message) (*pb.Message, error) {
	_, span := d.tele.Tracer.Start(ctx, "DHT.handlePrivateGetProviderRecords", otel.WithAttributes(attribute.String("remote", remote.String())))
	defer span.End()

	pirRequest := msg.GetEncryptedQuery()
	if pirRequest == nil {
		return nil, fmt.Errorf("no PIR Request sent in the message")
	}

	bucketsWithAddrInfos := d.normalizeRTJoinedWithPeerStore()

	encrypted_closer_peers, err := private_routing.RunPIRforCloserPeersRecords(pirRequest, bucketsWithAddrInfos)
	if err != nil {
		return nil, err
	}

	backend, ok := d.backends[namespaceProviders]
	if !ok {
		return nil, fmt.Errorf("unsupported record type: %s", namespaceProviders)
	}

	// TODO: Typecasted as I only implemented the method below for the providersBackend
	//  Can extend this to other backend types e.g. IPNS in the future.
	newBackend := backend.(*ProvidersBackend)
	mapCIDtoProviderPeers, err := newBackend.MapCIDsToProviderPeersForPIR(ctx)
	if err != nil {
		return nil, fmt.Errorf("could not construct a map of CIDs to provider peers for PIR")
	}

	encrypted_provider_peers, err := private_routing.RunPIRforProviderPeersRecords(pirRequest, mapCIDtoProviderPeers)
	pirResponse := &pb.PIR_Response{
		Id:            pirRequest.Id,
		CloserPeers:   encrypted_closer_peers,
		ProviderPeers: encrypted_provider_peers,
	}

	response := &pb.Message{
		Type:             pb.Message_PRIVATE_GET_PROVIDERS,
		EncryptedRecords: pirResponse,
	}

	return response, nil
}

// This function first normalizes the RT --- filling up any buckets that are not full with
// nearest nodes from other buckets, given only the common prefix length for that bucket.
// The (normalized) RT consists of <kad ID, peer ID> records.
// The d.host.Peerstore() consists of <peer ID, peer address> records.
// We then join these key-value stores here, oblivious to the target.
func (d *DHT) normalizeRTJoinedWithPeerStore() [][]*pb.Message_Peer {
	// TODO: How to extend this function to provide the functionality in Line 52 in handleFindPeer
	//  obliviously to the target key.
	// Line 52 in handleFindPeer looks up the peerstore with the target kademlia ID,
	// even though closerPeer looks up the peerStore with the output of
	// d.rt.NearestNodes(..)
	// See the comments there --- the rationale is that the target may be in a bucket
	// of the RT that was full, so we don't store its <kad ID, peer ID> in the RT.
	// But we still store its <peer ID, multiaddress array> in the peerstore.

	// So to do this obliviously to the target, we add some steps to *this function*,
	//  which is run before answering the PIR request.
	// We can fill up the RT with <kad ID, peer ID> of nodes that *are* in the peerstore but *not* in the RT.
	// This effectively adds more entries to the RT; some other than the target which may not have been there earlier.
	// So we need to ask Gui if this is acceptable. Let's suppose it is acceptable.
	// Then, we would've ensured that every <peer ID, multiaddress []> in the peerstore
	// has a corresponding <kad ID, peer ID> in the RT.

	// Then we would run NormalizeRT as seen below.
	// (Buckets might be very full already, so we might not need to normalize some buckets.)
	// Then the join will ensure that *any* target which was earlier in the peerstore,
	// but not in the RT, will *also* be included in the join output:
	// Join: <target's kadID, target's peerID> <target's peerID, target's address>

	// Bucket -> <Kad ID, Peer ID>
	bucketsWithPeerInfos := d.rt.NormalizeRT()

	bucketsWithAddrInfos := make([][]*pb.Message_Peer, 0, len(bucketsWithPeerInfos))

	// Bucket -> <Kad ID, Peer ID and multiaddress array>
	for bid, bucket := range bucketsWithPeerInfos {
		addrInfos := make([]*pb.Message_Peer, 0, len(bucket))
		for p := range bucket {
			pid := peer.ID(p)
			peerInfo := d.host.Peerstore().PeerInfo(pid)
			messagePeer := pb.FromAddrInfo(peerInfo)
			addrInfos = append(addrInfos, messagePeer)
		}
		bucketsWithAddrInfos[bid] = addrInfos
	}

	return bucketsWithAddrInfos
}
