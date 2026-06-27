package com.pigdawg;

import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.Message;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class ProducerConsumerThreads {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerConsumerThreads.class);

    private final StreamClient streamClient;
    private final String streamId;
    private final long producerDeadlineMs;
    private final long consumerDeadlineMs;
    private final AtomicInteger producedCount = new AtomicInteger(0);
    private final AtomicInteger consumedCount = new AtomicInteger(0);
    private final AtomicBoolean producerDone = new AtomicBoolean(false);
    private final AtomicReference<Exception> threadFailure = new AtomicReference<>();

    private String cursor;

    ProducerConsumerThreads(
            StreamClient streamClient,
            String streamId,
            String cursor,
            long producerDeadlineMs,
            long consumerDeadlineMs) {
        this.streamClient = streamClient;
        this.streamId = streamId;
        this.cursor = cursor;
        this.producerDeadlineMs = producerDeadlineMs;
        this.consumerDeadlineMs = consumerDeadlineMs;
    }

    int getProducedCount() {
        return producedCount.get();
    }

    int getConsumedCount() {
        return consumedCount.get();
    }

    void throwIfFailed() {
        Exception failure = threadFailure.get();
        if (failure == null) {
            return;
        }

        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }

        throw new IllegalStateException("Producer/consumer thread failed", failure);
    }

    Thread createProducerThread() {
        return new Thread(() -> {
            try {
                LOG.info("Producer thread started for stream={}", streamId);
                while (System.currentTimeMillis() < producerDeadlineMs) {
                    int sequence = producedCount.incrementAndGet();
                    String payload = "message-" + sequence;
                    OciStreamingHelper.publishMessage(streamClient, streamId, payload);
                    LOG.info("Produced: {}", payload);
                    Thread.sleep(200);
                }
            } catch (Exception ex) {
                rememberThreadFailure(ex);
                LOG.error("Producer thread error for stream={}", streamId, ex);
            } finally {
                producerDone.set(true);
                LOG.info("Producer thread exiting. totalProduced={}", producedCount.get());
            }
        }, "stream-producer");
    }

    Thread createConsumerThread() {
        return new Thread(() -> {
            try {
                LOG.info("Consumer thread started for stream={}", streamId);
                while (System.currentTimeMillis() < consumerDeadlineMs) {
                    GetMessagesResponse response = OciStreamingHelper.getMessages(streamClient, streamId, cursor, 10);

                    List<Message> messages = response.getItems();
                    if (messages != null && !messages.isEmpty()) {
                        LOG.debug("Consumer received {} messages in batch", messages.size());
                        for (Message message : messages) {
                            String value = new String(message.getValue(), StandardCharsets.UTF_8);
                            int totalConsumed = consumedCount.incrementAndGet();
                            LOG.info("Consumed ({}) : {}", totalConsumed, value);
                        }
                    } else {
                        LOG.debug("Consumer poll returned no messages; sleeping briefly");
                        Thread.sleep(200);
                    }

                    if (response.getOpcNextCursor() != null && !response.getOpcNextCursor().isEmpty()) {
                        cursor = response.getOpcNextCursor();
                    }

                    if (producerDone.get() && consumedCount.get() >= producedCount.get()) {
                        break;
                    }
                }

                if (producerDone.get() && consumedCount.get() < producedCount.get()) {
                    LOG.warn(
                            "Consumer reached timeout before full drain. produced={} consumed={}",
                            producedCount.get(),
                            consumedCount.get());
                }
                LOG.info("Consumer thread exiting. produced={} consumed={}", producedCount.get(), consumedCount.get());
            } catch (Exception ex) {
                rememberThreadFailure(ex);
                LOG.error("Consumer thread error for stream={}", streamId, ex);
            }
        }, "stream-consumer");
    }

    private void rememberThreadFailure(Exception failure) {
        Exception existingFailure = threadFailure.get();
        if (existingFailure != null) {
            existingFailure.addSuppressed(failure);
            return;
        }

        if (!threadFailure.compareAndSet(null, failure)) {
            threadFailure.get().addSuppressed(failure);
        }
    }
}
