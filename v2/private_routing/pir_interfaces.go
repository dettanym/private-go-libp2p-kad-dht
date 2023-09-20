package private_routing

import (
	"fmt"
	ds "github.com/ipfs/go-datastore"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
	"github.com/libp2p/go-libp2p/core/peerstore"
)

func RunPIRforCloserPeersRecords(req *pb.PIR_Request, addrBook peerstore.AddrBook) (*pb.PIR_Message, error) {
	pir_request := req.Key
	return nil, fmt.Errorf("could not compute PIR response over addressbook of peer records")
}

func RunPIRforProviderPeersRecords(req *pb.PIR_Request, addrBook peerstore.AddrBook, datastore ds.Datastore) (*pb.PIR_Message, error) {
	pir_request := req.Key
	// Maybe this method (runPIRforProviderPeerRecords) needs to be called from a PrivateFetch method on the Backend interface.
	// The PrivateFetch method can compute the join privately and then just run this method internally, returning the encrypted providerpeers.
	// The join would be computed as often as provider ads are requested.
	// We must do the join at least as frequent as the addressbook or the provider ads datastore is updated.
	return nil, fmt.Errorf("could not compute PIR response over datastore of provider records")
}
