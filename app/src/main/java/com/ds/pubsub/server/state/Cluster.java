package com.ds.pubsub.server.state;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ds.pubsub.common.Config.Host;
import com.ds.pubsub.proto.HeartbeatRequest;
import com.ds.pubsub.proto.HeartbeatResponse;
import com.ds.pubsub.proto.LeaderElectionServiceGrpc;
import com.ds.pubsub.proto.Node;
import com.ds.pubsub.proto.VoteRequest;
import com.ds.pubsub.proto.VoteResponse;
import com.ds.pubsub.server.clock.LogicalTimestamp;
import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Cluster {
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private final Node currentNode;
    private final List<Node> otherClusterNodes;
    private final List<Node> allNodes;
    private Map<String, ManagedChannel> channelMap;

    public Cluster(List<Node> otherClusterNodes, Node currentNode) {
      this.otherClusterNodes = otherClusterNodes;
      this.currentNode = currentNode;
      this.allNodes = new ArrayList<>(otherClusterNodes);
      allNodes.add(currentNode);
      this.channelMap = new HashMap<>();
    }

    public Node getCurrentNode() {
      return currentNode;
    }

    public List<Node> getOtherClusterNodes() {
      return this.otherClusterNodes;
    }

    public int getNodeCount() {
      return this.allNodes.size();
    }

    public Node getLeaderNode(String id) {
      return this.allNodes.stream()
          .filter(node -> node.getId().equals(id))
          .collect(Collectors.toList())
          .get(0);
    }

    public HeartbeatResponse sendHeartbeat(Node node, LogicalTimestamp currentTerm) {
      ManagedChannel channel = getChannel(node);
      LeaderElectionServiceGrpc.LeaderElectionServiceBlockingStub stub =
          LeaderElectionServiceGrpc.newBlockingStub(channel);
        
      HeartbeatRequest request =
                          HeartbeatRequest.newBuilder()
                              .setMemberId(getCurrentNode().getId())
                              .setTerm(ByteString.copyFrom(currentTerm.toBytes()))
                              .build();
      HeartbeatResponse response = null;
      try {
        response = stub.sendHeartbeat(request);
      } catch (Exception e) {
        logger.debug("", e);
        channelMap.get(node.getId()).shutdown();
        channelMap.remove(node.getId());
      }
      return response;
    }

  public VoteResponse requestVote(Node node, LogicalTimestamp currentTerm) {

    ManagedChannel channel = getChannel(node);
    LeaderElectionServiceGrpc.LeaderElectionServiceBlockingStub stub =
        LeaderElectionServiceGrpc.newBlockingStub(channel);
    VoteResponse response = null;
    VoteRequest request = VoteRequest.newBuilder().setCandidateId(getCurrentNode().getId()).setTerm(ByteString.copyFrom(currentTerm.toBytes())).build();
    logger.info(
      "member: [{}] sending vote request to: [{}:{}].",
      this.getCurrentNode().getId(), node.getIp(), node.getPort());
    try {
      response = stub.requestVote(request);
    } catch (Exception e) {
      logger.debug(e.getMessage(), e);
      channelMap.get(node.getId()).shutdown();
      channelMap.remove(node.getId());
    }

    return response;
  }

    public ManagedChannel getChannel(Node node) {
      if (!channelMap.containsKey(node.getId())) {
        ManagedChannel channel =
            ManagedChannelBuilder.forAddress(node.getIp(), node.getPort()).usePlaintext().build();
        channelMap.put(node.getId(), channel);
      }
      return channelMap.get(node.getId());
    }

    public static Cluster fromNodes(List<Host> nodes, int currentPort) { 
      InetAddress hostIpAddress = getLocalIpAddress();
      System.out.printf("Current node host: %s%n", hostIpAddress.getHostAddress());

      Node currentNode = null, tmpNode;
      List<Node> otherClusterNodes = new ArrayList<>();
      for (Host node : nodes) {
        tmpNode =
            Node.newBuilder()
                .setId(
                    String.valueOf(
                        UUID.nameUUIDFromBytes(
                            String.format("%s:%d", node.getIp(), node.getPort()).getBytes())))
                .setIp(node.getIp())
                .setPort(node.getPort())
                .build();
        if (node.getIp().equals(hostIpAddress.getHostAddress()) && node.getPort() == currentPort) {
          currentNode = tmpNode;
        } else {
          otherClusterNodes.add(tmpNode);
        }
      }
      if (currentNode == null)
        throw new RuntimeException("No node ip matches current running host ip");
      return new Cluster(otherClusterNodes, currentNode);
    }

    public static InetAddress getLocalIpAddress() {
      InetAddress ipAddress;
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress("google.com", 80));
        ipAddress = socket.getLocalAddress();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return ipAddress;
    }
}
