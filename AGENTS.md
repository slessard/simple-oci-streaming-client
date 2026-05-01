# AGENTS.md

Guidance for AI coding agents and future contributors working in this repository.

## Scope

These instructions apply to the entire repository.

## Project Overview

This is a small Java 17 Maven project named `simple-oci-streaming-client`. It demonstrates use of Oracle Cloud Infrastructure (OCI) Streaming by creating a stream pool and stream, producing and consuming messages, and cleaning up the created resources.

Key files:

- `pom.xml` — Maven build, Java version, dependencies, and `exec-maven-plugin` configuration.
- `src/main/java/com/pigdawg/SteamingApp.java` — application entry point and OCI Streaming demo logic.
- `src/main/resources/logback.xml` — Logback console logging configuration.
- `src/main/resources/application.properties` — local OCI configuration; intentionally ignored by Git.

## Build and Runtime Expectations

- Use Java 17. The Maven compiler release is configured with `<maven.compiler.release>17</maven.compiler.release>`.
- `mise.toml` pins the local toolchain to `java = "zulu-17"` and `maven = "3.9.6"`. If `java` or `mvn` are not on PATH, use `mise exec -- mvn compile` or `mise exec -- mvn test` for safe validation. On Apple Silicon Homebrew installs, `mise` may be available as `/opt/homebrew/bin/mise` even when it is not on PATH.
- Keep this as a Maven project; do not introduce another build system unless explicitly requested.
- Prefer safe validation commands that do not create cloud resources:
    - `mvn compile`
    - `mvn test` if tests are added later
    - `mise exec -- mvn compile` or `mise exec -- mvn test` when using the pinned `mise.toml` toolchain
- The app can be launched with `mvn exec:java` when Java is available and `exec-maven-plugin` points to `com.pigdawg.SteamingApp`.
- Do not run the application unless the user explicitly asks. Running it can create and delete OCI Streaming resources and requires valid local OCI configuration.
- If Java or Maven is unavailable, report that validation could not be completed rather than installing tools without permission.

## OCI Configuration and Secret Handling

- Never commit `src/main/resources/application.properties`; it is intentionally listed in `.gitignore`.
- Treat the following as sensitive and avoid committing, printing, or logging them:
    - OCI session tokens
    - API keys and private keys
    - tenancy/user/fingerprint values when they identify a private environment
    - compartment OCIDs, stream OCIDs, endpoints, or profile names unless the user has explicitly made them public/demo-safe
- Do not add sample credentials. If an example config is needed, create a sanitized template such as `application.properties.example` with placeholder values only.
- Do not make changes that broaden cloud permissions, alter authentication behavior, or run destructive OCI operations without explicit user approval.

## Java Code Style

- Keep code under the existing package namespace `com.pigdawg` unless a package rename is explicitly requested.
- Use Java 17 language features where they improve readability, but avoid unnecessary churn in this small demo project.
- Prefer clear method extraction for OCI operations, polling/waiting logic, and producer/consumer behavior.
- Use SLF4J parameterized logging (`LOG.info("value={}", value)`) instead of string concatenation in log statements.
- Avoid `System.out.println` in application logic; use the existing logger.
- Do not log secrets, tokens, or raw local configuration values.
- Preserve cleanup behavior for cloud resources. If changing lifecycle code, keep `try`/`finally` cleanup paths robust.
- Be careful with concurrent producer/consumer code; keep termination conditions deterministic and avoid unbounded loops.

## Maven Dependency Guidelines

- Keep dependency versions centralized in `<properties>` in `pom.xml`.
- Avoid adding dependencies for functionality already covered by the JDK or existing libraries.
- If adding OCI SDK modules, use the existing `${oci.sdk.version}` property.
- Do not upgrade dependency versions casually; explain why an upgrade is needed and validate compatibility.

## Logging Configuration

- `logback.xml` is configured for console output and a package-specific logger for `com.pigdawg`.
- Keep logger package names synchronized with Java package names.
- Prefer INFO for normal lifecycle messages and DEBUG for noisy polling or per-batch details.

## Testing and Validation

- There are currently no tests in the repository. For significant logic changes, consider adding tests under `src/test/java`.
- For changes that do not require live OCI access, validate with Maven compile/test commands when Java and Maven are available.
- For changes that require live OCI access, document what was not run and why unless the user explicitly authorizes a live run.

## Known Project Notes

- The main application class is `com.pigdawg.SteamingApp`.
- The class name appears to be spelled `SteamingApp`; do not rename it unless requested, because the Maven exec configuration depends on it.
- `src/main/resources/application.properties` is local-only and ignored by Git.
- The app manages real OCI Streaming resources; future agents should prioritize safe defaults and explicit user approval before live execution.

## Contribution Hygiene

- Keep changes focused and minimal.
- Before editing, check for existing user changes and avoid overwriting them.
- Do not commit generated build output such as `target/` or IDE files such as `.idea/`.
- Preserve the Apache-2.0 license unless the user explicitly requests licensing changes.
