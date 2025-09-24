---
theme: https://github.com/maaslalani/slides/raw/main/styles/theme.json
---

# ⚙️ Project: Stream Processing with Kafka & Flink

This presentation outlines the **learning-stream-processing** project, a pipeline built with Apache Kafka and Apache Flink to process a continuous stream of numerical data.

---

## 🏛️ High-Level Architecture

The system features a simple, linear data flow, moving data from a producer through Kafka and a Flink application to a final destination.

* **Producer**: A Rust app that generates a stream of numbers (0-12), intentionally mixed with corrupted data to simulate real-world errors.
* **`raw-topic` (Kafka Topic)**: Ingests the raw, unprocessed data from the producer.
* **Flink Application**: The core processing engine. It consumes from `raw-topic`, filters corrupted data, pairs numbers that sum to 12, and produces the results.
* **`enriched-topic` (Kafka Topic)**: Stores the final, processed pairs of numbers from the Flink application.

---

## 🛠️ Docker Components

The entire pipeline is orchestrated using a `docker-compose.yml` file, which defines and links the necessary services.

### 🐘 **Kafka**

* **Service**: `kafka`
* **Purpose**: The central **message broker**. It ingests data from the producer and stores the raw and enriched event streams in their respective topics.
* **Image**: `bitnami/kafka:latest`

### 🖥️ **Kafka UI**

* **Service**: `kafka-ui`
* **Purpose**: A **web UI** for monitoring and managing the Kafka cluster. It's perfect for inspecting topics and messages in real time.
* **Image**: `ghcr.io/kafbat/kafka-ui:latest`
* **Access**: `http://localhost:8080`

### ⚙️ **Flink Cluster**

* **JobManager (`jobmanager`)**: The **master node** that coordinates the application, schedules tasks, and manages state. The Flink Dashboard is available at `http://localhost:8081`.
* **TaskManager (`taskmanager`)**: A **worker node** that executes the actual data processing tasks as instructed by the JobManager.
* **Image**: `flink:2.0.0-scala_2.12-java11` for both.

---

## The Producer

The producer is a Rust application designed to simulate a real-world data stream by sending messages to a Kafka topic.

It generates two message types:

1. **Numeric messages**: A random number within a configured range.
2. **Corrupted messages**: A random alphanumeric string to simulate malformed data.

Key features include configurable message rates, numeric ranges, and error probabilities. It runs indefinitely and supports graceful shutdown.

---

## The Flink Application

The Flink application is the heart of the processing pipeline.

Its main job is to consume events from the `raw-topic` and find pairs of numbers that sum to 12. It also identifies any corrupted (non-numeric) data and routes it to a separate Dead Letter Queue (DLQ) topic for later analysis.

---

## Deserialization / Serialization

Kafka messages are just bytes. Our Flink application needs to understand them, which requires **deserialization** (bytes → objects) before processing and **serialization** (objects → bytes) when writing back.

```java

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
}
```

The custom `DeserializationResult` class is crucial here, as it allows us to gracefully handle parsing errors and route corrupted messages to a Dead Letter Queue (DLQ).

---

## Sources and Sinks

In Flink, **Sources** and **Sinks** are abstractions for interacting with external systems.

* A **Source** reads data *into* your Flink application.
* A **Sink** writes data *out* of it.

<!-- end list -->

```java
KafkaSource.<DeserializationResult<Event>>builder()
    .setBootstrapServers(bootstrapServers)
    .setTopics(rawTopic)
    .setGroupId(groupId)
    .setValueOnlyDeserializer(new JsonDeserializationSchema<>(Event.class))
    .build();

```

---

## Side Outputs

A standard Flink pipeline transforms one stream into another. But what if you need multiple output streams from a single step? That's where **Side Outputs** come in.

We use a side output to split the initial stream: valid events continue for processing, while raw, unparseable messages are sent to the DLQ.

```java

final OutputTag<byte[]> dlqTag = new OutputTag<>("dlq") {};

// ---------------------------------
public class DeserializationResultSplitter<T> extends 
  ProcessFunction<DeserializationResult<T>, T> {

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
// ---------------------------------


SingleOutputStreamOperator<Event> events =
    env.fromSource(source, WatermarkStrategy.noWatermarks(), "read-events")
        .process(new DeserializationResultSplitter<>(dlqTag))
        .returns(Event.class);

events.getSideOutput(dlqTag).sinkTo(dlqSink).name("dlq");
```

---

## Stateful Processing

To find pairs, our application must remember numbers it has already seen. This requires **state**.

Since Flink is a distributed system, a simple in-memory `Map` won't work. Flink provides robust, fault-tolerant state mechanisms like `ValueState` for this purpose.

```java
events
    .keyBy(event -> event.getPairKey(pairSum))
    .process(new PairProcessor(pairSum))
```

The code uses `keyBy` to group numbers that could form a pair (e.g., 3 and 9). The `PairProcessor` then uses `ValueState` to keep a running count for that key, emitting a pair whenever a match is found.

```java
public class PairProcessor extends KeyedProcessFunction<Long, Event, Pair> {
  private transient ValueState<Long> pairBalance;

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
```

---

## 🎬 DEMO

---

## ✅ Conclusion & Key Takeaways

* **Docker simplifies setup**: Getting a Kafka and Flink cluster running locally is straightforward with Docker Compose.
* **Producer performance**: Building a basic data producer is easy, but achieving high throughput requires more complex solutions.
* **Streaming vs. Batch**: Flink's streaming model introduces concepts like serialization and state management that differ from traditional batch processing.
* **Future improvements**: For a production system, adding a Schema Registry and implementing watermarks for event-time processing are critical next steps.
* **AI assistance**: Gemini was a helpful tool for learning and clarifying new concepts during development. 👍
