# simple-oci-streaming-client

This project is a small Java 17 demo for OCI Streaming.

It is meant to show the basic lifecycle of using the streaming service from a Java client: authenticate, create the needed resources, send messages, read them back, and clean up when finished. The app is intentionally simple and is useful as a working reference for OCI Streaming setup and request flow.

## Prerequisites

- Java 17
- Maven 3.9.x
- An OCI session token profile configured locally for the profile named in `application.properties`
- OCI permissions to create and delete stream pools and streams in the target compartment

## Configure the app

The program reads its runtime settings from `src/main/resources/application.properties`.

That file is intentionally not committed. Start from the example file in the same directory:

- [src/main/resources/example_application.properties](src/main/resources/example_application.properties)

Copy that file to `src/main/resources/application.properties`, then replace the placeholder values with real values for your environment.

Required properties:

- `oci.streaming.endpoint`
- `oci.identity.endpoint`
- `oci.streaming.tenancy`
- `oci.streaming.compartment`
- `oci.auth.profile`

## Run it

From the repository root:

```bash
mvn compile
mvn exec:java
```

`exec-maven-plugin` is already configured to run `com.pigdawg.SteamingApp`.

## What the test covers

When you run it, the app checks the full Streaming workflow:

1. It confirms the OCI session token and Identity endpoint work.
2. It creates a stream pool in the configured compartment.
3. It waits for that pool to become active.
4. It creates a stream inside the pool.
5. It waits for the stream to become active.
6. It creates a consumer group cursor at the start of the stream.
7. It starts one producer thread and one consumer thread.
8. The producer publishes messages to the stream.
9. The consumer reads messages back from the stream.
10. It deletes the stream, waits for deletion, deletes the pool, and waits for the pool to disappear.

Because the app manages live OCI resources, make sure the compartment and profile in `application.properties` point at the environment you expect.
