package org.mozdzonekmit.learning.flink.processing;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.mozdzonekmit.learning.flink.model.DeserializationResult;

public class DeserializationResultSplitter<T> extends ProcessFunction<DeserializationResult<T>, T> {

  private OutputTag<byte[]> dlqTag;

  public DeserializationResultSplitter(OutputTag<byte[]> dlqTag) {
    this.dlqTag = dlqTag;
  }

  @Override
  public void processElement(
      DeserializationResult<T> result,
      ProcessFunction<DeserializationResult<T>, T>.Context ctx,
      Collector<T> out)
      throws Exception {
    if (result.isSuccess()) out.collect(result.getEvent());
    else ctx.output(dlqTag, result.getRaw());
  }
}
