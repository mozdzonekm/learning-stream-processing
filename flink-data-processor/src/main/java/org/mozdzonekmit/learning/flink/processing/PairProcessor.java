package org.mozdzonekmit.learning.flink.processing;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.mozdzonekmit.learning.flink.model.Event;
import org.mozdzonekmit.learning.flink.model.Pair;

public class PairProcessor extends KeyedProcessFunction<Long, Event, Pair> {

  private static final long serialVersionUID = 1L;

  private transient ValueState<Long> pairBalance;

  private long pairSum;
  private long threshold;

  public PairProcessor(long pairSum) {
    this.pairSum = pairSum;
    this.threshold = pairSum / 2;
  }

  @Override
  public void open(OpenContext openContext) {
    ValueStateDescriptor<Long> pairBalanceDescriptor =
        new ValueStateDescriptor<>("pair-balance", Types.LONG);
    pairBalance = getRuntimeContext().getState(pairBalanceDescriptor);
  }

  @Override
  public void processElement(Event event, Context context, Collector<Pair> collector)
      throws Exception {
    long num = event.getNum();
    Long balance = pairBalance.value();

    if (balance == null) {
      balance = 0L;
    }

    if (num < threshold) {
      if (balance > 0) collector.collect(new Pair(num, pairSum - num));
      balance--;
    } else if (num > threshold) {
      if (balance < 0) collector.collect(new Pair(pairSum - num, num));
      balance++;
    } else {
      balance++;
      if (balance >= 2) {
        balance -= 2;
        collector.collect(new Pair(num, num));
      }
    }
    pairBalance.update(balance);
  }
}
