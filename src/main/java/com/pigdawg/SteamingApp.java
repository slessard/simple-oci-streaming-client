package com.pigdawg;

import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.Message;
import com.oracle.bmc.streaming.responses.CreateCursorResponse;
import com.oracle.bmc.streaming.responses.CreateStreamPoolResponse;
import com.oracle.bmc.streaming.responses.CreateStreamResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SteamingApp {
    private static final Logger LOG = LoggerFactory.getLogger(SteamingApp.class);

    private static final String APPLICATION_PROPERTIES = "application.properties";
    private static final Properties APP_PROPERTIES = loadApplicationProperties();

    public static final String ENDPOINT_OCI_STREAMING = getRequiredProperty("oci.streaming.endpoint");
    public static final String ENDPOINT_OCI_IDENTITY = getRequiredProperty("oci.identity.endpoint");

    public static final String TENANT_STREAMING = getRequiredProperty("oci.streaming.tenancy");

    public static final String COMPARTMENT_DELTA = getRequiredProperty("oci.streaming.compartment");

    private static final String AUTH_PROFILE_DELTA = getRequiredProperty("oci.auth.profile");

    private static final long RESOURCE_WAIT_TIMEOUT_MS = 20_000;


    private static Properties loadApplicationProperties() {
        try (InputStream inputStream = SteamingApp.class.getClassLoader().getResourceAsStream(APPLICATION_PROPERTIES)) {
            if (inputStream == null) {
                throw new IllegalStateException("Unable to find " + APPLICATION_PROPERTIES + " on the classpath");
            }

            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        } catch (IOException ex) {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private static String getRequiredProperty(String key) {
        String value = APP_PROPERTIES.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("Missing required property: " + key);
        }

        return value;
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        LOG.info("SteamingApp starting");
        java.time.LocalDateTime now = java.time.LocalDateTime.now();
        java.time.format.DateTimeFormatter formatter =
                java.time.format.DateTimeFormatter.ofPattern("yyyyMMddHHmm");
        final String nowString = now.format(formatter);

        final String poolName = String.format("stevel-pool-%s", nowString);
        final String streamName = String.format("stevel-stream-%s", nowString);
        final String consumerGroupName = String.format("stevel-consumergroup-%s", nowString);
        final String cursorName = String.format("stevel-cursor-%s", nowString);
        LOG.info("Generated names pool={} stream={} consumerGroup={} cursor={}", poolName, streamName, consumerGroupName, cursorName);

        // Streams are assigned a specific endpoint url based on where they are provisioned.
        // Create a stream client using the provided message endpoint.
        StreamClient streamClient = OciStreamingHelper.createStreamClient(ENDPOINT_OCI_STREAMING, AUTH_PROFILE_DELTA);
        StreamAdminClient adminClient = OciStreamingHelper.createStreamAdminClient(ENDPOINT_OCI_STREAMING, AUTH_PROFILE_DELTA);
        LOG.info("Initialized StreamClient and StreamAdminClient for endpoint={}", ENDPOINT_OCI_STREAMING);

        CreateStreamPoolResponse createStreamPoolResponse = null;
        try {
            LOG.info("Creating stream pool {} in compartment {}", poolName, COMPARTMENT_DELTA);
            createStreamPoolResponse = OciStreamingHelper.createStreamPool(adminClient, COMPARTMENT_DELTA, poolName);
            LOG.info("Created stream pool id={}", createStreamPoolResponse.getStreamPool().getId());

            // Wait for stream pool to be created
            OciStreamingHelper.waitForStreamPoolToBecomeActive(adminClient, createStreamPoolResponse.getStreamPool().getId(), RESOURCE_WAIT_TIMEOUT_MS);

            CreateStreamResponse createStreamResponse = null;
            try {
                createStreamResponse = OciStreamingHelper.createStream(adminClient, createStreamPoolResponse.getStreamPool().getId(), streamName);
                String streamId = createStreamResponse.getStream().getId();
                LOG.info("Created stream id={} in pool={}", streamId, createStreamPoolResponse.getStreamPool().getId());

                // Wait for the stream to be created
                OciStreamingHelper.waitForStreamToBecomeActive(adminClient, streamId, RESOURCE_WAIT_TIMEOUT_MS);

                // Produce and Consumer messages
                final long startMs = System.currentTimeMillis();
                final long producerDeadlineMs = startMs + 10_000;
                final long consumerDeadlineMs = startMs + 15_000;

                CreateCursorResponse createCursorResponse = OciStreamingHelper.createCursor(streamClient, streamId);
                String cursor = createCursorResponse.getCursor().getValue();

                ProducerConsumerThreads producerConsumerThreads =
                        new ProducerConsumerThreads(
                                streamClient,
                                streamId,
                                cursor,
                                producerDeadlineMs,
                                consumerDeadlineMs);
                Thread producerThread = producerConsumerThreads.createProducerThread();
                Thread consumerThread = producerConsumerThreads.createConsumerThread();

                LOG.info("Starting producer and consumer threads for stream={}", streamId);
                producerThread.start();
                consumerThread.start();
                producerThread.join();
                consumerThread.join();
                LOG.info(
                        "Threads completed. produced={} consumed={}",
                        producerConsumerThreads.getProducedCount(),
                        producerConsumerThreads.getConsumedCount());
            } finally {
                if (createStreamResponse != null) {
                    LOG.info("Deleting stream id={}", createStreamResponse.getStream().getId());
                    OciStreamingHelper.deleteStream(adminClient, createStreamResponse.getStream().getId());

                    // wait for the stream to be deleted
                    OciStreamingHelper.waitForStreamToBecomeDeleted(adminClient, createStreamResponse.getStream().getId(), RESOURCE_WAIT_TIMEOUT_MS);
                }
            }
        } finally {
            if (createStreamPoolResponse != null) {
                LOG.info("Deleting stream pool id={}", createStreamPoolResponse.getStreamPool().getId());
                OciStreamingHelper.deleteStreamPool(adminClient, createStreamPoolResponse.getStreamPool().getId());
            }
        }

//        // A cursor can be created as part of a consumer group.
//        // Committed offsets are managed for the group, and partitions
//        // are dynamically balanced amongst consumers in the group.
//        LOG.info("Starting a simple message loop with a group cursor");
//        CreateGroupCursorResponse groupCursorResponse = OciStreamingHelper.createGroupCursor(streamClient, streamId, consumerGroupName, cursorName);
//        simpleReadMessagesLoop(streamClient, streamId, groupCursorResponse.getCursor().getValue());

        LOG.info("SteamingApp done");
    }

    private static void simpleReadMessagesLoop(StreamClient streamClient, String streamId, String cursor)
            throws InterruptedException {
        String currentCursor = cursor;
        LOG.info("Starting simple read loop streamId={}", streamId);

        for (int i = 0; i < 20; i++) {
            GetMessagesResponse response = OciStreamingHelper.getMessages(streamClient, streamId, currentCursor, 10);

            List<Message> messages = response.getItems();
            if (messages == null || messages.isEmpty()) {
                LOG.debug("Simple loop poll returned no messages");
                Thread.sleep(1000);
            } else {
                for (Message message : messages) {
                    String value = new String(message.getValue(), StandardCharsets.UTF_8);
                    LOG.info(
                            "partition={} offset={} key={} value={}",
                            message.getPartition(),
                            message.getOffset(),
                            message.getKey(),
                            value);
                }
            }

            if (response.getOpcNextCursor() == null || response.getOpcNextCursor().isEmpty()) {
                break;
            }

            currentCursor = response.getOpcNextCursor();
        }
        LOG.info("Simple read loop complete streamId={}", streamId);
    }
}
