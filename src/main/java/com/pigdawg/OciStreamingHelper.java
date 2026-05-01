package com.pigdawg;

import com.google.common.collect.ImmutableMap;
import com.oracle.bmc.auth.SessionTokenAuthenticationDetailsProvider;
import com.oracle.bmc.identity.IdentityClient;
import com.oracle.bmc.identity.model.Compartment;
import com.oracle.bmc.identity.requests.ListCompartmentsRequest;
import com.oracle.bmc.identity.responses.ListCompartmentsResponse;
import com.oracle.bmc.streaming.StreamAdminClient;
import com.oracle.bmc.streaming.StreamClient;
import com.oracle.bmc.streaming.model.CreateCursorDetails;
import com.oracle.bmc.streaming.model.CreateGroupCursorDetails;
import com.oracle.bmc.streaming.model.CreateStreamDetails;
import com.oracle.bmc.streaming.model.CreateStreamPoolDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetails;
import com.oracle.bmc.streaming.model.PutMessagesDetailsEntry;
import com.oracle.bmc.streaming.model.Stream;
import com.oracle.bmc.streaming.model.StreamPool;
import com.oracle.bmc.streaming.model.StreamSummary;
import com.oracle.bmc.streaming.model.UpdateStreamDetails;
import com.oracle.bmc.streaming.requests.CreateCursorRequest;
import com.oracle.bmc.streaming.requests.CreateGroupCursorRequest;
import com.oracle.bmc.streaming.requests.CreateStreamPoolRequest;
import com.oracle.bmc.streaming.requests.CreateStreamRequest;
import com.oracle.bmc.streaming.requests.DeleteStreamPoolRequest;
import com.oracle.bmc.streaming.requests.DeleteStreamRequest;
import com.oracle.bmc.streaming.requests.GetMessagesRequest;
import com.oracle.bmc.streaming.requests.GetStreamPoolRequest;
import com.oracle.bmc.streaming.requests.GetStreamRequest;
import com.oracle.bmc.streaming.requests.ListStreamsRequest;
import com.oracle.bmc.streaming.requests.PutMessagesRequest;
import com.oracle.bmc.streaming.requests.UpdateStreamRequest;
import com.oracle.bmc.streaming.responses.CreateCursorResponse;
import com.oracle.bmc.streaming.responses.CreateGroupCursorResponse;
import com.oracle.bmc.streaming.responses.CreateStreamPoolResponse;
import com.oracle.bmc.streaming.responses.CreateStreamResponse;
import com.oracle.bmc.streaming.responses.DeleteStreamPoolResponse;
import com.oracle.bmc.streaming.responses.DeleteStreamResponse;
import com.oracle.bmc.streaming.responses.GetMessagesResponse;
import com.oracle.bmc.streaming.responses.GetStreamPoolResponse;
import com.oracle.bmc.streaming.responses.GetStreamResponse;
import com.oracle.bmc.streaming.responses.ListStreamsResponse;
import com.oracle.bmc.streaming.responses.PutMessagesResponse;
import com.oracle.bmc.streaming.responses.UpdateStreamResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class OciStreamingHelper {
    private static final Logger LOG = LoggerFactory.getLogger(OciStreamingHelper.class);

    private static final long RESOURCE_WAIT_POLL_INTERVAL_MS = 2_000;

    private OciStreamingHelper() {
    }

    public static StreamClient createStreamClient(String streamingEndpoint, String authProfile) throws IOException {
        return StreamClient.builder()
                .endpoint(streamingEndpoint)
                .build(new SessionTokenAuthenticationDetailsProvider(authProfile));
    }

    public static StreamAdminClient createStreamAdminClient(String streamingEndpoint, String authProfile) throws IOException {
        return StreamAdminClient.builder()
                .endpoint(streamingEndpoint)
                .build(new SessionTokenAuthenticationDetailsProvider(authProfile));
    }

    public static IdentityClient createIdentityClient(String identityEndpoint, String authProfile) throws IOException {
        return IdentityClient.builder()
                .endpoint(identityEndpoint)
                .build(new SessionTokenAuthenticationDetailsProvider(authProfile));
    }

    public static UpdateStreamResponse updateStream(StreamAdminClient adminClient, String streamId) {
        UpdateStreamRequest updateStreamRequest = UpdateStreamRequest.builder()
                .streamId(streamId)
                .updateStreamDetails(UpdateStreamDetails.builder()
                        .freeformTags(ImmutableMap.of("test", "value"))
                        .build())
                .build();

        return adminClient.updateStream(updateStreamRequest);
    }

    public static GetStreamResponse getStream(StreamAdminClient adminClient, String streamId) {
        return adminClient.getStream(GetStreamRequest.builder()
                .streamId(streamId)
                .build());
    }

    public static void waitForStreamPoolToBecomeActive(StreamAdminClient adminClient, String streamPoolId, long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

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

            Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream pool " + streamPoolId + " to become ACTIVE");
    }

    public static void waitForStreamToBecomeActive(StreamAdminClient adminClient, String streamId, long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

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

            Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream " + streamId + " to become ACTIVE");
    }

    public static void waitForStreamToBecomeDeleted(StreamAdminClient adminClient, String streamId, long resourceWaitTimeoutMs)
            throws InterruptedException {
        final long deadline = System.currentTimeMillis() + resourceWaitTimeoutMs;

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

            Thread.sleep(RESOURCE_WAIT_POLL_INTERVAL_MS);
        }

        throw new IllegalStateException(
                "Timed out waiting for stream " + streamId + " to become DELETED");
    }

    public static GetStreamPoolResponse getStreamPool(StreamAdminClient adminClient, String streamPoolId) {
        return adminClient.getStreamPool(
                GetStreamPoolRequest.builder()
                        .streamPoolId(streamPoolId)
                        .build());
    }

    public static DeleteStreamPoolResponse deleteStreamPool(StreamAdminClient adminClient, String streamPoolId) {
        LOG.info("Deleting stream pool via API streamPoolId={}", streamPoolId);
        return adminClient.deleteStreamPool(DeleteStreamPoolRequest.builder().streamPoolId(streamPoolId).build());
    }

    public static CreateStreamPoolResponse createStreamPool(
            StreamAdminClient adminClient,
            String compartmentId,
            String poolName) {
        LOG.info("Creating stream pool via API. compartmentId={} poolName={}", compartmentId, poolName);
        return adminClient.createStreamPool(CreateStreamPoolRequest.builder()
                .createStreamPoolDetails(CreateStreamPoolDetails.builder()
                        .compartmentId(compartmentId)
                        .name(poolName)
                        .build())
                .build());
    }

    public static CreateStreamResponse createStream(
            StreamAdminClient adminClient,
            String poolId,
            String streamName) {
        LOG.info("Creating stream via API. poolId={} streamName={}", poolId, streamName);
        return adminClient.createStream(CreateStreamRequest.builder()
                .createStreamDetails(CreateStreamDetails.builder()
                        .streamPoolId(poolId)
                        .name(streamName)
                        .partitions(1)
                        .build())
                .build());
    }

    public static void deleteStreams(List<StreamSummary> streamSummaries, StreamAdminClient adminClient) {
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

    public static DeleteStreamResponse deleteStream(StreamAdminClient adminClient, String streamId) {
        LOG.info("Deleting stream via API streamId={}", streamId);
        return adminClient.deleteStream(DeleteStreamRequest.builder().streamId(streamId).build());
    }

    public static List<StreamSummary> listStreams(
            StreamAdminClient adminClient,
            String compartmentId,
            Stream.LifecycleState lifecycleState) {
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

    public static List<String> listCompartmentOcidsInTenancy(
            String identityEndpoint,
            String authProfile,
            String tenancyOcid) throws IOException {
        LOG.info("Listing compartment OCIDs for tenancy={}", tenancyOcid);

        IdentityClient identityClient = createIdentityClient(identityEndpoint, authProfile);

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

    public static CreateGroupCursorResponse createGroupCursor(
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

        return streamClient.createGroupCursor(
                CreateGroupCursorRequest.builder()
                        .streamId(streamId)
                        .createGroupCursorDetails(groupCursorDetails)
                        .build());
    }

    public static CreateCursorResponse createCursor(StreamClient streamClient, String streamId) {
        LOG.debug("Creating cursor for streamId={}", streamId);
        CreateCursorDetails cursorDetails = CreateCursorDetails.builder()
                .type(CreateCursorDetails.Type.TrimHorizon)
                .partition("0")
                .build();

        return streamClient.createCursor(
                CreateCursorRequest.builder()
                        .streamId(streamId)
                        .createCursorDetails(cursorDetails)
                        .build());
    }

    public static PutMessagesResponse publishMessage(StreamClient streamClient, String streamId, String value) {
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

    public static GetMessagesResponse getMessages(StreamClient streamClient, String streamId, String cursor, int limit) {
        return streamClient.getMessages(
                GetMessagesRequest.builder()
                        .streamId(streamId)
                        .cursor(cursor)
                        .limit(limit)
                        .build());
    }
}