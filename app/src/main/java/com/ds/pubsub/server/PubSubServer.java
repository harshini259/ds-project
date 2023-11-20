package com.ds.pubsub.server;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Duration;

import com.ds.pubsub.common.Config;
import com.ds.pubsub.proto.Node;
import com.ds.pubsub.server.services.LeaderElectionService;
import com.ds.pubsub.server.state.Cluster;
import io.grpc.Server;
import io.grpc.ServerBuilder;

public class PubSubServer {
    private final static Logger log = LoggerFactory.getLogger(PubSubServer.class);

    Cluster cluster;
    RaftLeaderElection leaderElection;

    PubSubServer(Cluster cluster) {
        this.cluster = cluster;
        this.leaderElection = new RaftLeaderElection(cluster);
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String confFilename = args[0];
        int port = Integer.parseInt(args[1]);

        Config config = Config.loadConfig(confFilename);
        Cluster cluster = Cluster.fromNodes(config.getNodes(), port);
        PubSubServer server = new PubSubServer(cluster);
        server.start();
    }

    public void start() throws IOException, InterruptedException {
        Node currNode = this.cluster.getCurrentNode();
        Server server =
            ServerBuilder.forPort(currNode.getPort())
                .addService(new LeaderElectionService(this.leaderElection))
                .build();
        server.start();
        log.info(
            String.format("Server started: %s", Arrays.toString(server.getListenSockets().toArray())));

        this.leaderElection.start();
        server.awaitTermination();
    }
}
