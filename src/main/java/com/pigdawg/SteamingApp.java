package com.pigdawg;

import com.google.common.collect.ImmutableMap;
import com.oracle.bmc.auth.SessionTokenAuthenticationDetailsProvider;
import com.oracle.bmc.identity.IdentityClient;
import com.oracle.bmc.identity.model.Compartment;
import com.oracle.bmc.identity.requests.ListCompartmentsRequest;
import com.oracle.bmc.identity.responses.ListCompartmentsResponse;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.CreateStreamDetails;
import com.oracle.bmc.streaming.model.CreateStreamPoolDetails;
import com.oracle.bmc.streaming.model.CreateCursorDetails;
import com.oracle.bmc.streaming.model.Message;
import com.oracle.bmc.streaming.model.Stream;
import com.oracle.bmc.streaming.model.StreamPool;
import com.oracle.bmc.streaming.model.StreamSummary;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.UpdateStreamDetails;
import com.oracle.bmc.streaming.requests.CreateCursorRequest;
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest;
import com.oracle.bmc.streaming.requests.CreateStreamPoolRequest;
import com.oracle.bmc.streaming.requests.CreateStreamRequest;
import com.oracle.bmc.streaming.requests.DeleteStreamPoolRequest;
import com.oracle.bmc.streaming.requests.DeleteStreamRequest;
import com.oracle.bmc.streaming.requests.GetStreamPoolRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.requests.UpdateStreamRequest;
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse;
import com.oracle.bmc.streaming.responses.CreateCursorResponse;
import com.oracle.bmc.streaming.responses.CreateStreamPoolResponse;
import com.oracle.bmc.streaming.responses.CreateStreamResponse;
import com.oracle.bmc.streaming.responses.DeleteStreamPoolResponse;
import com.oracle.bmc.streaming.responses.DeleteStreamResponse;
import com.oracle.bmc.streaming.responses.GetStreamPoolResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;
import com.oracle.bmc.streaming.responses.UpdateStreamResponse;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.checkerframework.checker.nullness.qual.NonNull;
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
        StreamClient streamClient = StreamClient.builder()
                .endpoint(ENDPOINT_OCI_STREAMING)
                .build(new SessionTokenAuthenticationDetailsProvider(AUTH_PROFILE_DELTA));

        StreamAdminClient adminClient = StreamAdminClient.builder()
                .endpoint(ENDPOINT_OCI_STREAMING)
                .build(new SessionTokenAuthenticationDetailsProvider(AUTH_PROFILE_DELTA));
        LOG.info("Initialized StreamClient and StreamAdminClient for endpoint={}", ENDPOINT_OCI_STREAMING);

        CreateStreamPoolResponse createStreamPoolResponse = null;
        try {
            LOG.info("Creating stream pool {} in compartment {}", poolName, COMPARTMENT_DELTA);
            createStreamPoolResponse = createStreamPool(adminClient, COMPARTMENT_DELTA, poolName);
            LOG.info("Created stream pool id={}", createStreamPoolResponse.getStreamPool().getId());

            // Wait for stream pool to be created
            waitForStreamPoolToBecomeActive(adminClient, createStreamPoolResponse.getStreamPool().getId());

            CreateStreamResponse createStreamResponse = null;
            try {
                createStreamResponse = createStream(adminClient, createStreamPoolResponse.getStreamPool().getId(), streamName);
                String streamId = createStreamResponse.getStream().getId();
                LOG.info("Created stream id={} in pool={}", streamId, createStreamPoolResponse.getStreamPool().getId());

                // Wait for the stream to be created
                waitForStreamToBecomeActive(adminClient, streamId);

                // Produce and Consumer messages
                final long startMs = System.currentTimeMillis();
                final long producerDeadlineMs = startMs + 10_000;
                final long consumerDeadlineMs = startMs + 15_000;
                final AtomicInteger producedCount = new AtomicInteger(0);
                final AtomicInteger consumedCount = new AtomicInteger(0);
                final AtomicBoolean producerDone = new AtomicBoolean(false);

                Thread producerThread = createProducerThread(producerDeadlineMs, producedCount, streamClient, streamId, producerDone);
                Thread consumerThread = createConsumerThread(streamClient, streamId, consumerDeadlineMs, consumedCount, producerDone, producedCount);

                LOG.info("Starting producer and consumer threads for stream={}", streamId);
                producerThread.start();
                consumerThread.start();
                producerThread.join();
                consumerThread.join();
                LOG.info("Threads completed. produced={} consumed={}", producedCount.get(), consumedCount.get());
            }
            finally {
                if (createStreamResponse != null) {
                    LOG.info("Deleting stream id={}", createStreamResponse.getStream().getId());
                    deleteStream(adminClient, createStreamResponse.getStream().getId());

                    // wait for the stream to be deleted
                    waitForStreamToBecomeDeleted(adminClient, createStreamResponse.getStream().getId());
                }
            }
        }
        finally {
            if (createStreamPoolResponse != null) {
                LOG.info("Deleting stream pool id={}", createStreamPoolResponse.getStreamPool().getId());
                deleteStreamPool(adminClient, createStreamPoolResponse.getStreamPool().getId());
            }
        }

//        // A cursor can be created as part of a consumer group.
//        // Committed offsets are managed for the group, and partitions
//        // are dynamically balanced amongst consumers in the group.
//        System.out.println("Starting a simple message loop with a group cursor");
//        CreateGroupCursorResponse groupCursorResponse = getCursorByGroup(streamClient, streamId, consumerGroupName, cursorName);
//        simpleReadMessagesLoop(streamClient, streamId, groupCursorResponse.getCursor().getValue());


        LOG.info("SteamingApp done");
    }

    private static @NonNull Thread createConsumerThread(StreamClient streamClient, String streamId, long consumerDeadlineMs, AtomicInteger consumedCount, AtomicBoolean producerDone, AtomicInteger producedCount) {
        Thread consumerThread = new Thread(() -> {
            try {
                LOG.info("Consumer thread started for stream={}", streamId);
                String currentCursor = createCursor(streamClient, streamId).getCursor().getValue();
                LOG.debug("Consumer cursor created for stream={}", streamId);
                while (System.currentTimeMillis() < consumerDeadlineMs) {
                    GetMessagesResponse response = streamClient.getMessages(
                            GetMessagesRequest.builder()
                                    .streamId(streamId)
                                    .cursor(currentCursor)
                                    .limit(10)
                                    .build());

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
                        currentCursor = response.getOpcNextCursor();
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
                LOG.error("Consumer thread error for stream={}", streamId, ex);
            }
        }, "stream-consumer");
        return consumerThread;
    }

    private static @NonNull Thread createProducerThread(long producerDeadlineMs, AtomicInteger producedCount, StreamClient streamClient, String streamId, AtomicBoolean producerDone) {
        Thread producerThread = new Thread(() -> {
            try {
                LOG.info("Producer thread started for stream={}", streamId);
                while (System.currentTimeMillis() < producerDeadlineMs) {
                    int sequence = producedCount.incrementAndGet();
                    String payload = "message-" + sequence;
                    publishMessage(streamClient, streamId, payload);
                    LOG.info("Produced: {}", payload);
                    Thread.sleep(200);
                }
            } catch (Exception ex) {
                LOG.error("Producer thread error for stream={}", streamId, ex);
            } finally {
                producerDone.set(true);
                LOG.info("Producer thread exiting. totalProduced={}", producedCount.get());
            }
        }, "stream-producer");
        return producerThread;
    }

    private static UpdateStreamResponse updateStream(StreamAdminClient adminClient, String streamId) {
        UpdateStreamRequest updateStreamRequest = UpdateStreamRequest.builder()
                .streamId(streamId)
                .updateStreamDetails(UpdateStreamDetails.builder()
                        .freeformTags(ImmutableMap.of("test", "value"))
                        .build())
                .build();

        UpdateStreamResponse updateStreamResponse = adminClient.updateStream(updateStreamRequest);
        return updateStreamResponse;
    }

    private static GetStreamResponse getStream(StreamAdminClient adminClient, String streamId) {
        GetStreamResponse getStreamResponse = adminClient.getStream(GetStreamRequest.builder()
                .streamId(streamId)
                .build());
        return getStreamResponse;
    }

    private static void waitForStreamPoolToBecomeActive(StreamAdminClient adminClient, String streamPoolId)
            throws InterruptedException {
        final long timeoutMs = 20_000;
        final long pollIntervalMs = 2_000;
        final long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            GetStreamPoolResponse response = getStreamPool(adminClient, streamPoolId);

            StreamPool.LifecycleState state = response.getStreamPool().getLifecycleState();
            LOG.debug("Polling stream pool state. streamPoolId={} state={}", streamPoolId, state);
            if (StreamPool.LifecycleState.Active.equals(state)) {
                LOG.info("Stream pool is ACTIVE. streamPoolId={}", streamPoolId);
                return;
            }

            if (StreamPool.LifecycleState.Failed.equals(state) || StreamPool.LifecycleState.Deleted.equals(state)) {
                throw new IllegalStateException(
                        "Stream pool " + streamPoolId + " entered terminal state: " + state);
            }

            Thread.sleep(pollIntervalMs);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream pool " + streamPoolId + " to become ACTIVE");
    }

    private static void waitForStreamToBecomeActive(StreamAdminClient adminClient, String streamId)
            throws InterruptedException {
        final long timeoutMs = 20_000;
        final long pollIntervalMs = 2_000;
        final long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            GetStreamResponse response = getStream(adminClient, streamId);

            Stream.LifecycleState state = response.getStream().getLifecycleState();
            LOG.debug("Polling stream state. streamId={} state={}", streamId, state);
            if (Stream.LifecycleState.Active.equals(state)) {
                LOG.info("Stream is ACTIVE. streamId={}", streamId);
                return;
            }

            if (Stream.LifecycleState.Failed.equals(state) || Stream.LifecycleState.Deleted.equals(state)) {
                throw new IllegalStateException(
                        "Stream " + streamId + " entered terminal state: " + state);
            }

            Thread.sleep(pollIntervalMs);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream " + streamId + " to become ACTIVE");
    }

    private static void waitForStreamToBecomeDeleted(StreamAdminClient adminClient, String streamId)
            throws InterruptedException {
        final long timeoutMs = 20_000;
        final long pollIntervalMs = 2_000;
        final long deadline = System.currentTimeMillis() + timeoutMs;

        while (System.currentTimeMillis() < deadline) {
            GetStreamResponse response = getStream(adminClient, streamId);

            Stream.LifecycleState state = response.getStream().getLifecycleState();
            LOG.debug("Polling stream deletion state. streamId={} state={}", streamId, state);
            if (Stream.LifecycleState.Deleted.equals(state)) {
                LOG.info("Stream is DELETED. streamId={}", streamId);
                return;
            }

            if (Stream.LifecycleState.Failed.equals(state)) {
                throw new IllegalStateException(
                        "Stream " + streamId + " entered terminal state: " + state);
            }

            Thread.sleep(pollIntervalMs);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream " + streamId + " to become DELETED");
    }

    private static GetStreamPoolResponse getStreamPool(StreamAdminClient adminClient, String streamPoolId) {
        GetStreamPoolResponse response = adminClient.getStreamPool(
                GetStreamPoolRequest.builder()
                        .streamPoolId(streamPoolId)
                        .build());
        return response;
    }

    private static DeleteStreamPoolResponse deleteStreamPool(StreamAdminClient adminClient, String streamPoolId) {
        LOG.info("Deleting stream pool via API streamPoolId={}", streamPoolId);
        DeleteStreamPoolResponse deleteStreamPoolResponse =
                adminClient.deleteStreamPool(DeleteStreamPoolRequest.builder().streamPoolId(streamPoolId).build());
        return deleteStreamPoolResponse;
    }

    private static CreateStreamPoolResponse createStreamPool(
            StreamAdminClient adminClient,
            String compartmentId,
            String poolName) {
        LOG.info("Creating stream pool via API. compartmentId={} poolName={}", compartmentId, poolName);
        CreateStreamPoolResponse response = adminClient.createStreamPool(CreateStreamPoolRequest.builder()
                .createStreamPoolDetails(CreateStreamPoolDetails.builder()
                        .compartmentId(compartmentId)
                        .name(poolName)
                        .build())
                .build());
        return response;
    }

    private static CreateStreamResponse createStream(
            StreamAdminClient adminClient,
            String poolId,
            String streamName) {
        LOG.info("Creating stream via API. poolId={} streamName={}", poolId, streamName);
        CreateStreamResponse response = adminClient.createStream(CreateStreamRequest.builder()
                .createStreamDetails(CreateStreamDetails.builder()
                        .streamPoolId(poolId)
                        .name(streamName)
                        .partitions(1)
                        .build())
                .build());
        return response;
    }

    private static void deleteStreams(List<StreamSummary> streamSummaries, StreamAdminClient adminClient) {
        for (StreamSummary streamSummary : streamSummaries) {
            LOG.info("Trying to delete streamId={}", streamSummary.getId());
            try {
                deleteStream(adminClient, streamSummary.getId());
            } catch (Exception ex) {
                LOG.error("Failed deleting streamId={}", streamSummary.getId(), ex);
            }
            LOG.info("Deleted streamId={}", streamSummary.getId());
        }
    }

    private static DeleteStreamResponse deleteStream(StreamAdminClient adminClient, String streamId) {
        LOG.info("Deleting stream via API streamId={}", streamId);
        DeleteStreamResponse deleteStreamResponse =
                adminClient.deleteStream(DeleteStreamRequest.builder().streamId(streamId).build());
        return deleteStreamResponse;
    }

    private static List<StreamSummary> listStreams(StreamAdminClient adminClient, String compartmentId, Stream.LifecycleState lifecycleState) {
        List<StreamSummary> streamSummaries = new ArrayList<>();
        String page = null;

        do {
            ListStreamsResponse listStreamsResponse = adminClient.listStreams(ListStreamsRequest.builder()
                    .compartmentId(compartmentId)
                    .lifecycleState(lifecycleState)
                    .page(page)
                    .build());
            LOG.info("List streams page fetched. pageSize={} lifecycleState={} compartmentId={}",
                    listStreamsResponse.getItems().size(), lifecycleState, compartmentId);
            streamSummaries.addAll(listStreamsResponse.getItems());

            page = listStreamsResponse.getOpcNextPage();
        } while (page != null);
        LOG.info("Total streams collected={}", streamSummaries.size());

        return streamSummaries;
    }

    private static List<String> listCompartmentOcidsInTenancy(String tenancyOcid) throws IOException {
        LOG.info("Listing compartment OCIDs for tenancy={}", tenancyOcid);
        SessionTokenAuthenticationDetailsProvider authenticationDetailsProvider = new SessionTokenAuthenticationDetailsProvider(AUTH_PROFILE_DELTA);

        IdentityClient identityClient = IdentityClient.builder()
                .endpoint(ENDPOINT_OCI_IDENTITY)
                .build(authenticationDetailsProvider);

        try {
            List<String> compartmentOcids = new ArrayList<>();
            String page = null;

            do {
                ListCompartmentsRequest request = ListCompartmentsRequest.builder()
                        .compartmentId(tenancyOcid)
                        .compartmentIdInSubtree(Boolean.TRUE)
                        .accessLevel(ListCompartmentsRequest.AccessLevel.Accessible)
                        .page(page)
                        .limit(1000)
                        .build();

                ListCompartmentsResponse response = identityClient.listCompartments(request);
                LOG.debug("Fetched compartment page size={}", response.getItems().size());

                for (Compartment compartment : response.getItems()) {
                    compartmentOcids.add(compartment.getId());
                }

                page = response.getOpcNextPage();
            } while (page != null);

            return compartmentOcids;
        } finally {
            LOG.debug("Closing identity client");
            identityClient.close();
        }
    }

    private static CreateGroupCursorResponse createGroupCursor(
            StreamClient streamClient,
            String streamId,
            String groupName,
            String instanceName) {
        LOG.info("Creating group cursor. streamId={} groupName={} instanceName={}", streamId, groupName, instanceName);
        CreateGroupCursorDetails groupCursorDetails = CreateGroupCursorDetails.builder()
                .groupName(groupName)
                .instanceName(instanceName)
                .type(CreateGroupCursorDetails.Type.TrimHorizon)
                .timeoutInMs(30000)
                .build();

        CreateGroupCursorResponse response = streamClient.createGroupCursor(
                CreateGroupCursorRequest.builder()
                        .streamId(streamId)
                        .createGroupCursorDetails(groupCursorDetails)
                        .build());

        return response;
    }

    private static CreateCursorResponse createCursor(StreamClient streamClient, String streamId) {
        LOG.debug("Creating cursor for streamId={}", streamId);
        CreateCursorDetails cursorDetails = CreateCursorDetails.builder()
                .type(CreateCursorDetails.Type.TrimHorizon)
                .partition("0")
                .build();

        CreateCursorResponse response = streamClient.createCursor(
                CreateCursorRequest.builder()
                        .streamId(streamId)
                        .createCursorDetails(cursorDetails)
                        .build());

        return response;
    }

    private static PutMessagesResponse publishMessage(StreamClient streamClient, String streamId, String value) {
        LOG.debug("Publishing message to streamId={} payload={}", streamId, value);
        String encodedValue = Base64.getEncoder().encodeToString(value.getBytes(StandardCharsets.UTF_8));

        PutMessagesDetailsEntry entry = PutMessagesDetailsEntry.builder()
                .key("demo-key".getBytes(StandardCharsets.UTF_8))
                .value(encodedValue.getBytes(StandardCharsets.UTF_8))
                .build();

        PutMessagesDetails details = PutMessagesDetails.builder()
                .messages(List.of(entry))
                .build();

        return streamClient.putMessages(
                PutMessagesRequest.builder()
                        .streamId(streamId)
                        .putMessagesDetails(details)
                        .build());
    }

    private static void simpleReadMessagesLoop(StreamClient streamClient, String streamId, String cursor)
            throws InterruptedException {
        String currentCursor = cursor;
        LOG.info("Starting simple read loop streamId={}", streamId);

        for (int i = 0; i < 20; i++) {
            GetMessagesResponse response = streamClient.getMessages(
                    GetMessagesRequest.builder()
                            .streamId(streamId)
                            .cursor(currentCursor)
                            .limit(10)
                            .build());

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