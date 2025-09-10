package org.mozdzonekmit.learning.flink.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.SerializationSchema;

public class JsonSerializationSchema<T> implements SerializationSchema<T> {
  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public byte[] serialize(T value) {
    try {
      return mapper.writeValueAsBytes(value);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
