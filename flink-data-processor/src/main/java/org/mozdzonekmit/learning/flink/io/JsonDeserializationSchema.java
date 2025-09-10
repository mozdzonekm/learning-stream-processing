package org.mozdzonekmit.learning.flink.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.mozdzonekmit.learning.flink.model.DeserializationResult;

public class JsonDeserializationSchema<T>
    implements DeserializationSchema<DeserializationResult<T>> {

  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final Class<T> clazz;

  public JsonDeserializationSchema(Class<T> clazz) {
    this.clazz = clazz;
  }

  @Override
  public DeserializationResult<T> deserialize(byte[] message) {
    try {
      T item = objectMapper.readValue(message, clazz);
      return new DeserializationResult<>(item, message, null);
    } catch (Exception e) {
      return new DeserializationResult<>(null, message, e);
    }
  }

  @Override
  public boolean isEndOfStream(DeserializationResult<T> nextElement) {
    return false;
  }

  @Override
  public TypeInformation<DeserializationResult<T>> getProducedType() {
    return TypeInformation.of(new TypeHint<DeserializationResult<T>>() {});
  }
}
