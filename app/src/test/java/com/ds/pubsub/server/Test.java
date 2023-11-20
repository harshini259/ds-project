package com.ds.pubsub.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ds.pubsub.proto.HelloMessage;
import com.ds.pubsub.proto.HelloResponse;
import com.ds.pubsub.proto.HelloServiceGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Test {
    private final static Logger log = LoggerFactory.getLogger(PubSubServer.class);

    public static void main(String[] args){
        ManagedChannel channel =
        ManagedChannelBuilder.forAddress("10.0.0.95", 6000).usePlaintext().build();
        HelloServiceGrpc.HelloServiceBlockingStub stub =
            HelloServiceGrpc.newBlockingStub(channel);
        HelloMessage request =
                            HelloMessage.newBuilder()
                                .setId("10.0.0.95")
                                .build();
        HelloResponse response = null;
        try {
            response = stub.hello(request);
            log.info("Resonse:  "+ response.getMessage());
        } catch (Exception e) {
            log.error("Error in sending Hello", e);
        }
    }
}
