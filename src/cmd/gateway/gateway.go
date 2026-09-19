package main

import (
	"context"

	"battleworld/pb"
	gatewaywire "battleworld/transport/gateway"
)

func (s *GatewayServer) GetGatewayStatus(_ context.Context, _ *pb.GatewayStatusRequest) (*pb.GatewayStatusResponse, error) {
	gatewayStatus := s.gameCluster.GatewayStatus()
	response := &pb.GatewayStatusResponse{
		RoutingReady:    gatewayStatus.RoutingReady,
		TopologyVersion: gatewayStatus.TopologyVersion,
		Summary:         gatewayStatus.Summary,
		Nodes:           make([]*pb.NodeView, 0, len(gatewayStatus.Nodes)),
	}
	for _, node := range gatewayStatus.Nodes {
		response.Nodes = append(response.Nodes, gatewaywire.ToNodeView(node))
	}
	return response, nil
}
