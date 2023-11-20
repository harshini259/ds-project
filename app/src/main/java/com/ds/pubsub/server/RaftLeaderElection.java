package com.ds.pubsub.server;

import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ds.pubsub.server.clock.LogicalClock;
import com.ds.pubsub.server.clock.LogicalTimestamp;
import com.ds.pubsub.server.state.Cluster;
import com.ds.pubsub.server.state.JobScheduler;
import com.ds.pubsub.server.state.StateMachine;
import com.google.protobuf.ByteString;

import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.ds.pubsub.proto.HeartbeatRequest;
import com.ds.pubsub.proto.HeartbeatResponse;
import com.ds.pubsub.proto.Leader;
import com.ds.pubsub.proto.Node;
import com.ds.pubsub.proto.State;
import com.ds.pubsub.proto.VoteRequest;
import com.ds.pubsub.proto.VoteResponse;

public class RaftLeaderElection {
    private static final Logger logger = LoggerFactory.getLogger(RaftLeaderElection.class);

    private static final int HEART_BEAT_INTERVAL_MS = 200;
    private static final int MAX_TIMEOUT_MS = 800;

    private final Cluster cluster;
    private final StateMachine stateMachine;
    private final LogicalClock clock = new LogicalClock();
    private final AtomicReference<LogicalTimestamp> currentTerm = new AtomicReference<>();

    public final JobScheduler timeoutScheduler;
    public final JobScheduler heartbeatScheduler;
    private JobScheduler leaderScheduler;

    private final AtomicReference<String> currentLeader = new AtomicReference<>("");

    private int timeoutMs;

    public RaftLeaderElection(Cluster cluster) {

        this.timeoutMs = 
        new Random().nextInt(MAX_TIMEOUT_MS - (MAX_TIMEOUT_MS / 2)) + (MAX_TIMEOUT_MS / 2);
        
        this.cluster = cluster;
        
        // Initalize state machine.
        this.stateMachine =
        StateMachine.builder()
            .init(State.INACTIVE)
            .addTransition(State.INACTIVE, State.FOLLOWER)
            .addTransition(State.FOLLOWER, State.CANDIDATE)
            .addTransition(State.CANDIDATE, State.LEADER)
            .addTransition(State.LEADER, State.FOLLOWER)
            .build();

        this.stateMachine.on(State.FOLLOWER, becomeFollower());
        this.stateMachine.on(State.CANDIDATE, becomeCandidate());
        this.stateMachine.on(State.LEADER, becomeLeader());
        this.currentTerm.set(clock.tick());
        this.timeoutScheduler = new JobScheduler(onHeartbeatNotReceived());
        this.heartbeatScheduler = new JobScheduler(sendHeartbeat());  
    }

    public void restTimeoutScheduler() {
        this.timeoutScheduler.reset(this.timeoutMs);
    }
    
    public String getMemberId() {
        return this.cluster.getCurrentNode().getId();
    }

    public LogicalTimestamp currentTerm() {
        return this.currentTerm.get();
    }

    public void start() {
        this.stateMachine.transition(State.FOLLOWER, currentTerm.get());
    }

    private Consumer onHeartbeatNotReceived() {
        return toCandidate -> {
          this.timeoutScheduler.stop();
          this.currentTerm.set(clock.tick(currentTerm.get()));
          this.stateMachine.transition(State.CANDIDATE, currentTerm.get());
          logger.info(
              "member: [{}] didn't receive heartbeat until timeout: [{} ms] became: [{}]",
              this.getMemberId(),
              timeoutMs,
              stateMachine.currentState());
        };
    }

    private Consumer sendHeartbeat() {
        return heartbeat ->
            cluster
                .getOtherClusterNodes()
                .forEach(
                    node -> {
                      logger.info(
                          "node: [{}] with address [{}:{}] sending heartbeat",
                          node.getId(),
                          node.getIp(),
                          node.getPort());
                      HeartbeatResponse response = this.cluster.sendHeartbeat(node, this.currentTerm.get());
                      if (response == null) {
                        logger.info(
                            "Error requesting for heartbeat response from node: {}, address: {}:{}",
                            node.getId(),
                            node.getIp(),
                            node.getPort());
                      } else {
                        LogicalTimestamp term =
                            LogicalTimestamp.fromBytes(response.getTerm().toByteArray());
                        if (currentTerm.get().isBefore(term)) {
                          logger.debug(
                              "member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
                              this.getMemberId(),
                              currentTerm.get(),
                              term);
                          currentTerm.set(term);
                        }
                      }
                    });
      }

      private Consumer becomeLeader() {
        return leader -> {
          logger.info("member: [{}] has become: [{}].", this.getMemberId(), stateMachine.currentState());

          timeoutScheduler.stop();
          heartbeatScheduler.start(HEART_BEAT_INTERVAL_MS);
          this.currentLeader.set(this.getMemberId());

          CompletableFuture.runAsync(this::onBecomeLeader);
        };
    }

    public void onBecomeLeader() {
        logger.info(
            this.getMemberId()
                + " ("
                + this.currentTerm().toLong()
                + ") >>>>>>>    +++ Become A Leader +++");
        leaderScheduler = new JobScheduler(leaderIsWorking());
        leaderScheduler.start(1000);
    }

    private Consumer<Object> leaderIsWorking() {
        return doingSomeWork -> {
          logger.info("{}: I am working...", this.getMemberId());
        };
    }
      
    private Consumer becomeCandidate() {
        return election -> {
            logger.info("member: [{}] has become: [{}].", this.getMemberId(), stateMachine.currentState());

            heartbeatScheduler.stop();
            currentTerm.set(clock.tick());
            sendElectionCampaign();
            
            CompletableFuture.runAsync(this::onBecomeCandidate);
        };
    }

    
    public void onBecomeCandidate() {
        logger.info(this.getMemberId() + " (" + this.currentTerm().toLong() + ") ?? Become A Candidate");
        leaderScheduler.stop();
    }

    private void sendElectionCampaign() {
        CountDownLatch consensus =
            new CountDownLatch(((this.cluster.getNodeCount() + 1) / 2));
        logger.info("Number of nodes: [{}]", ((this.cluster.getNodeCount() + 1) / 2));
        this.cluster
            .getOtherClusterNodes()
            .forEach(
                node -> {
                VoteResponse response = this.cluster.requestVote(node, currentTerm.get());
                logger.info("Vote response: [{}] [{}]", node, response);
                if (response == null) {
                    logger.debug(
                        "Error requesting for vote response from node: {}, address: {}:{}",
                        node.getId(), node.getIp(), node.getPort());
                } else {
                    logger.debug("member: [{}] received vote response: [{}].", this.getMemberId(), response);
                        if (response.getGranted()) {
                            consensus.countDown();
                        }
                    }
                });
        try {
        boolean countZero = consensus.await(5, TimeUnit.SECONDS);
        stateMachine.transition(State.LEADER, currentTerm);
        } catch (InterruptedException e) {
        stateMachine.transition(State.FOLLOWER, currentTerm);
        }
    }

      /** node became follower when it initiates */
    private Consumer becomeFollower() {
        return follower -> {
            logger.info("member: [{}] has become: [{}].", this.getMemberId(), stateMachine.currentState());

            heartbeatScheduler.stop();
            timeoutScheduler.start(this.timeoutMs);
            CompletableFuture.runAsync(this::onBecomeFollower);
        };
    }

    public void onBecomeFollower() {
        logger.info(this.getMemberId() + " (" + this.currentTerm().toLong() + ") << Become A Follower");
        leaderScheduler.stop();
    }


    public HeartbeatResponse handleSendHeartBeat(HeartbeatRequest request) {
        logger.info("member [{}] received heartbeat request: [{}]", this.getMemberId(), request);
        restTimeoutScheduler();

        LogicalTimestamp term = LogicalTimestamp.fromBytes(request.getTerm().toByteArray());
        if (currentTerm.get().isBefore(term)) {
            logger.info( "member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.", this.getMemberId(), currentTerm.get(), term);
            currentTerm.set(term);
        }

        if (!stateMachine.currentState().equals(State.FOLLOWER)) {
            logger.debug(
                "member [{}] currentState [{}] and received heartbeat. becoming FOLLOWER.", this.getMemberId(), stateMachine.currentState());
            stateMachine.transition(State.FOLLOWER, term);
        }

        this.currentLeader.set(request.getMemberId());

        return HeartbeatResponse.newBuilder()
            .setMemberId(this.getMemberId())
            .setTerm(ByteString.copyFrom(currentTerm.get().toBytes()))
            .build();
    }


    public VoteResponse handleGetVote(VoteRequest request) {
        LogicalTimestamp term = LogicalTimestamp.fromBytes(request.getTerm().toByteArray());

        boolean voteGranted = currentTerm.get().isBefore(term);
        logger.info(
            "member [{}:{}] received vote request: [{}] voteGranted: [{}].",
            this.getMemberId(),
            stateMachine.currentState(),
            request,
            voteGranted);
    
        if (currentTerm.get().isBefore(term)) {
          logger.info(
              "member: [{}] currentTerm: [{}] is before: [{}] setting new seen term.",
              this.getMemberId(),
              currentTerm.get(),
              term);
          currentTerm.set(term);
        }
        return VoteResponse.newBuilder().setGranted(voteGranted).setCandidateId(this.getMemberId()).build();
    }

    public Node getLeaderNode() {
        return this.cluster.getLeaderNode(this.currentLeader.get());
    }

    public Leader leader() {
        return Leader.newBuilder()
            .setLeaderId(this.currentLeader.get())
            .setMemberId(this.getMemberId())
            .build();
    }
}
