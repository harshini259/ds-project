package com.ds.pubsub.common;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import lombok.*;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Config {
  String app;
  String version;
  String topicDir;
  List<Host> nodes;

  @Getter
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Host {
    String ip;
    int port;
  }

 public static Config loadConfig(String filename) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    mapper.findAndRegisterModules();
    return mapper.readValue(new File(filename), Config.class);
 }
}
