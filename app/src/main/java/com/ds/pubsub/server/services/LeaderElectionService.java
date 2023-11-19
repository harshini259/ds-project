package com.ds.pubsub.server.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ds.pubsub.proto.LeaderElectionServiceGrpc;
import com.ds.pubsub.server.RaftLeaderElection;

public class LeaderElectionService extends LeaderElectionServiceGrpc.LeaderElectionServiceImplBase {
    RaftLeaderElection raftLeaderElection;
  private final Logger logger = LoggerFactory.getLogger(RaftLeaderElection.class);

    public LeaderElectionService(RaftLeaderElection raftLeaderElection) {
        this.raftLeaderElection = raftLeaderElection;
    }
    
    /**
     * Returns current Leader.
     */
    public void getLeader(com.ds.pubsub.proto.EmptyMessage request,
        io.grpc.stub.StreamObserver<com.ds.pubsub.proto.Leader> responseObserver) {
        responseObserver.onNext(raftLeaderElection.leader());
        responseObserver.onCompleted();
    }

    /**
     * Sends Heart beat to other nodes.
     */
    public void sendHeartbeat(com.ds.pubsub.proto.HeartbeatRequest request,
        io.grpc.stub.StreamObserver<com.ds.pubsub.proto.HeartbeatResponse> responseObserver) {
        responseObserver.onNext(raftLeaderElection.handleSendHeartBeat(request));
        responseObserver.onCompleted();
    }

    /**
     * Request heart beat from other nodes.
     */
    public void requestVote(com.ds.pubsub.proto.VoteRequest request,
        io.grpc.stub.StreamObserver<com.ds.pubsub.proto.VoteResponse> responseObserver) {
        responseObserver.onNext(raftLeaderElection.handleGetVote(request));
        responseObserver.onCompleted();
    }
}
