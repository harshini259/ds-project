package com.ds.pubsub.server;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ds.pubsub.proto.HelloMessage;
import com.ds.pubsub.proto.HelloResponse;
import com.ds.pubsub.proto.HelloServiceGrpc;
import com.ds.pubsub.server.state.Cluster;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public  class HelloService extends HelloServiceGrpc.HelloServiceImplBase {
    private final static Logger log = LoggerFactory.getLogger(HelloService.class);

    Cluster cluster;

    HelloService(Cluster cluster){
        this.cluster = cluster;
    }
    public void hello(com.ds.pubsub.proto.HelloMessage request,
        io.grpc.stub.StreamObserver<com.ds.pubsub.proto.HelloResponse> responseObserver) {
        log.info("Got hello request from {}", request.getId());
        responseObserver.onNext(HelloResponse.newBuilder().setMessage("Hello from " + this.cluster.getCurrentNode().getIp()).build());
        responseObserver.onCompleted();
    }

    public Consumer pingOtherServers() {
        return anything -> {
            this.cluster.getOtherClusterNodes().forEach(node -> {
            ManagedChannel channel =
            ManagedChannelBuilder.forAddress(node.getIp(), node.getPort()).usePlaintext().build();
            log.info("Node ip {} port {} {} {}", node.getIp(), node.getPort(), node.getIp().equals("10.0.0.95"), node.getPort() == 6000);
            HelloServiceGrpc.HelloServiceBlockingStub stub =
                HelloServiceGrpc.newBlockingStub(channel).withDeadlineAfter(10, TimeUnit.SECONDS);;
            HelloMessage request =
                                HelloMessage.newBuilder()
                                    .setId(""+this.cluster.getCurrentNode().getPort())
                                    .build();
            HelloResponse response = null;
            try {
                response = stub.hello(request);
                log.info("Resonse:  "+ response.getMessage());
            } catch (Exception e) {
                log.error("Error in sending Hello");
            }
            channel.shutdown();
            });
        };
    }
}