package private_routing

import (
	"fmt"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

func runPIRforCloserPeersRecords(req *pb.PIR_Request, addrBook peerstore.AddrBook) (*pb.PIR_Message, error) {
	pir_request := req.Key
	return nil, fmt.Errorf("could not compute PIR response over addressbook of peer records")
}

func runPIRforProviderPeersRecords(req *pb.PIR_Request, addrBook peerstore.AddrBook, datastore ds.Datastore) (*pb.PIR_Message, error) {
	pir_request := req.Key
	// can compute join here between datastore and the addressbook
	// it'd run as often as provider ads are requested.
	// We must do the join at least as frequent as the addressbook or the provider ads datastore is updated.
	return nil, fmt.Errorf("could not compute PIR response over datastore of provider records")
}

// Computes the PIR response to a PIR request in a private FindNode message.
func private_FindNode(request *pb.PIR_Request, addrBook peerstore.AddrBook) (*pb.PIR_Response, error) {
	// Do CPIR over datastore
	encrypted_peer_ids, err := runPIRforCloserPeersRecords(request, addrBook)
	if err != nil {
		return nil, err
	}
	response := &pb.PIR_Response{
		Id:          request.Id,
		CloserPeers: encrypted_peer_ids,
	}
	return response, nil
}

// Computes the PIR response to a PIR request in a private GetProviders message.
func private_GetProviderRecords(request *pb.PIR_Request, addrBook peerstore.AddrBook, datastore ds.Datastore) (*pb.PIR_Response, error) {
	// Then use the record as an index to do CPIR over addrBook.
	encrypted_closer_peers, err := runPIRforCloserPeersRecords(request, addrBook)
	if err != nil {
		return nil, err
	}

	encrypted_provider_peers, err := runPIRforProviderPeersRecords(request, addrBook, datastore)
	response := &pb.PIR_Response{
		Id:            request.Id,
		CloserPeers:   encrypted_closer_peers,
		ProviderPeers: encrypted_provider_peers,
	}
	return response, nil
}
