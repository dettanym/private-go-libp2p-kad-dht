package private_routing

import (
	"fmt"
	"github.com/libp2p/go-libp2p-kad-dht/v2/pb"
)

func RunPIRforCloserPeersRecords(req *pb.PIR_Request, ModifiedRT [][]*pb.Message_Peer) (*pb.PIR_Message, error) {
	pir_request := req.Key
	return nil, fmt.Errorf("could not compute PIR response over ModifiedRT of peer records")
}

func RunPIRforProviderPeersRecords(req *pb.PIR_Request, mapCIDtoProviderPeers map[string][]*pb.Message_Peer) (*pb.PIR_Message, error) {
	pir_request := req.Key
	// Maybe this method (runPIRforProviderPeerRecords) needs to be called from a PrivateFetch method on the Backend interface.
	// The PrivateFetch method can compute the join privately and then just run this method internally, returning the encrypted providerpeers.
	// The join would be computed as often as provider ads are requested.
	// We must do the join at least as frequent as the addressbook or the provider ads datastore is updated.
	return nil, fmt.Errorf("could not compute PIR response over mapCIDtoProviderPeers")
}
