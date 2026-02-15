package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

/**
 * Comprehensive tests for the MutinyToReactor combined recipe.
 * Tests complex chained scenarios that involve multiple transformation types.
 */
class MutinyToReactorTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new MutinyToReactor())
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsComplexUniChainWithCreateItemAndErrorHandling() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class UserService {
            Uni<String> getUser(String id) {
              return Uni.createFrom().item(findUser(id))
                .onItem().transform(user -> user.toUpperCase())
                .onItem().invoke(user -> log(user))
                .onFailure().retry().atMost(3)
                .onFailure().invoke(e -> logError(e))
                .onFailure().recoverWithItem("ANONYMOUS");
            }

            private String findUser(String id) { return id; }
            private void log(String s) {}
            private void logError(Throwable e) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class UserService {
            Mono<String> getUser(String id) {
              return Mono.just(findUser(id)).map(user -> user.toUpperCase()).doOnNext(user -> log(user)).retry(3).doOnError(e -> logError(e)).onErrorReturn("ANONYMOUS");
            }

            private String findUser(String id) { return id; }
            private void log(String s) {}
            private void logError(Throwable e) {}
          }
          """
      )
    );
  }

  @Test
  void transformsComplexMultiChainWithCreateItemsAndErrorHandling() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class EventService {
            Multi<String> processEvents() {
              return Multi.createFrom().items("event1", "event2", "event3")
                .onItem().transform(e -> e.toUpperCase())
                .onItem().invoke(e -> log(e))
                .onFailure().invoke(err -> logError(err))
                .onFailure().recoverWithItem("ERROR_EVENT");
            }

            private void log(String s) {}
            private void logError(Throwable e) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Flux;

          class EventService {
            Flux<String> processEvents() {
              return Flux.just("event1", "event2", "event3").map(e -> e.toUpperCase()).doOnNext(e -> log(e)).doOnError(err -> logError(err)).onErrorReturn("ERROR_EVENT");
            }

            private void log(String s) {}
            private void logError(Throwable e) {}
          }
          """
      )
    );
  }

  @Test
  void transformsUniWithFlatMapAndErrorHandling() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class OrderService {
            Uni<String> processOrder(String orderId) {
              return Uni.createFrom().item(orderId)
                .onItem().transformToUni(id -> validateOrder(id))
                .onItem().invoke(order -> audit(order))
                .onFailure().recoverWithItem(e -> "ORDER_FAILED");
            }

            private Uni<String> validateOrder(String id) {
              return Uni.createFrom().item("validated_" + id);
            }

            private void audit(String order) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class OrderService {
            Mono<String> processOrder(String orderId) {
              return Mono.just(orderId).flatMap(id -> validateOrder(id)).doOnNext(order -> audit(order)).onErrorResume(e -> Mono.just("ORDER_FAILED"));
            }

            private Mono<String> validateOrder(String id) {
              return Mono.just("validated_" + id);
            }

            private void audit(String order) {}
          }
          """
      )
    );
  }

  @Test
  void transformsUniToMultiTransformationWithErrorHandling() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import io.smallrye.mutiny.Uni;

          class DataService {
            Multi<String> expandData(String input) {
              return Uni.createFrom().item(input)
                .onItem().transformToMulti(s -> Multi.createFrom().items(s, s.toUpperCase(), s.toLowerCase()))
                .onItem().invoke(s -> log(s))
                .onFailure().recoverWithItem("ERROR");
            }

            private void log(String s) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Flux;

          class DataService {
            Flux<String> expandData(String input) {
              return Mono.just(input).flatMapMany(s -> Flux.just(s, s.toUpperCase(), s.toLowerCase())).doOnNext(s -> log(s)).onErrorReturn("ERROR");
            }

            private void log(String s) {}
          }
          """
      )
    );
  }

  @Test
  void transformsMultiWithFlatMapAndDelayAndRetry() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import io.smallrye.mutiny.Uni;
          import java.time.Duration;

          class StreamService {
            Multi<String> processStream() {
              return Multi.createFrom().items("a", "b", "c")
                .onItem().transformToUni(s -> enrichItem(s))
                .onItem().delayIt().by(Duration.ofMillis(100))
                .onFailure().retry().atMost(5);
            }

            private Uni<String> enrichItem(String s) {
              return Uni.createFrom().item("enriched_" + s);
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Flux;
          import reactor.core.publisher.Mono;

          import java.time.Duration;

          class StreamService {
            Flux<String> processStream() {
              return Flux.just("a", "b", "c").flatMap(s -> enrichItem(s)).delayElement(Duration.ofMillis(100)).retry(5);
            }

            private Mono<String> enrichItem(String s) {
              return Mono.just("enriched_" + s);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniWithNullHandlingAndErrorRecovery() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class ConfigService {
            Uni<String> getConfig(String key) {
              return Uni.createFrom().item(findConfig(key))
                .onItem().ifNull().continueWith("default-config")
                .onItem().transform(c -> c.trim())
                .onFailure().recoverWithItem("fallback-config");
            }

            private String findConfig(String key) { return null; }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class ConfigService {
            Mono<String> getConfig(String key) {
              return Mono.just(findConfig(key)).switchIfEmpty(Mono.just("default-config")).map(c -> c.trim()).onErrorReturn("fallback-config");
            }

            private String findConfig(String key) { return null; }
          }
          """
      )
    );
  }

  @Test
  void transformsMultiWithIgnoreAndContinueWith() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class WorkflowService {
            Multi<String> executeWorkflow() {
              return Multi.createFrom().items("step1", "step2", "step3")
                .onItem().invoke(step -> execute(step))
                .onItem().ignore().andContinueWith("COMPLETED")
                .onFailure().recoverWithItem("FAILED");
            }

            private void execute(String step) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Flux;

          class WorkflowService {
            Flux<String> executeWorkflow() {
              return Flux.just("step1", "step2", "step3").doOnNext(step -> execute(step)).thenReturn("COMPLETED").onErrorReturn("FAILED");
            }

            private void execute(String step) {}
          }
          """
      )
    );
  }

  @Test
  void transformsUniWithRetryBackoffAndRecovery() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;
          import java.time.Duration;

          class ApiService {
            Uni<String> callExternalApi(String endpoint) {
              return Uni.createFrom().item(makeRequest(endpoint))
                .onItem().invoke(response -> log(response))
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(5))
                .onFailure().recoverWithItem("API_ERROR");
            }

            private String makeRequest(String endpoint) { return "response"; }
            private void log(String s) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          import java.time.Duration;

          class ApiService {
            Mono<String> callExternalApi(String endpoint) {
              return Mono.just(makeRequest(endpoint)).doOnNext(response -> log(response)).retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100)).maxBackoff(Duration.ofSeconds(5))).onErrorReturn("API_ERROR");
            }

            private String makeRequest(String endpoint) { return "response"; }
            private void log(String s) {}
          }
          """
      )
    );
  }

  @Test
  void transformsComplexMultiToUniTransformation() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import io.smallrye.mutiny.Uni;

          class AggregationService {
            Multi<String> aggregateData() {
              return Multi.createFrom().items("data1", "data2", "data3")
                .onItem().transformToUni(data -> processAsync(data))
                .onItem().transform(result -> result.toUpperCase())
                .onFailure().invoke(e -> logError(e))
                .onFailure().recoverWithItem("AGGREGATION_FAILED");
            }

            private Uni<String> processAsync(String data) {
              return Uni.createFrom().item("processed_" + data);
            }

            private void logError(Throwable e) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Flux;
          import reactor.core.publisher.Mono;

          class AggregationService {
            Flux<String> aggregateData() {
              return Flux.just("data1", "data2", "data3").flatMap(data -> processAsync(data)).map(result -> result.toUpperCase()).doOnError(e -> logError(e)).onErrorReturn("AGGREGATION_FAILED");
            }

            private Mono<String> processAsync(String data) {
              return Mono.just("processed_" + data);
            }

            private void logError(Throwable e) {}
          }
          """
      )
    );
  }

  @Test
  void transformsUniWithDelayAndMultipleErrorHandlers() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;
          import java.time.Duration;

          class NotificationService {
            Uni<String> sendNotification(String message) {
              return Uni.createFrom().item(message)
                .onItem().transform(m -> m.toUpperCase())
                .onItem().delayIt().by(Duration.ofMillis(500))
                .onItem().invoke(m -> logSending(m))
                .onFailure().invoke(e -> logError(e))
                .onFailure().retry().atMost(3)
                .onFailure().recoverWithItem("NOTIFICATION_FAILED");
            }

            private void logSending(String m) {}
            private void logError(Throwable e) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          import java.time.Duration;

          class NotificationService {
            Mono<String> sendNotification(String message) {
              return Mono.just(message).map(m -> m.toUpperCase()).delayElement(Duration.ofMillis(500)).doOnNext(m -> logSending(m)).doOnError(e -> logError(e)).retry(3).onErrorReturn("NOTIFICATION_FAILED");
            }

            private void logSending(String m) {}
            private void logError(Throwable e) {}
          }
          """
      )
    );
  }

  @Test
  void transformsMultiWithTransformToMultiAndErrorHandling() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class DataExpansionService {
            Multi<Integer> expandAndProcess(int start) {
              return Multi.createFrom().items(start, start + 1, start + 2)
                .onItem().transformToMulti(n -> Multi.createFrom().range(n, 3))
                .onItem().transform(n -> n * 2)
                .onItem().invoke(n -> log(n))
                .onFailure().recoverWithItem(-1);
            }

            private void log(int n) {}
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Flux;

          class DataExpansionService {
            Flux<Integer> expandAndProcess(int start) {
              return Flux.just(start, start + 1, start + 2).flatMapMany(n -> Flux.range(n, 3)).map(n -> n * 2).doOnNext(n -> log(n)).onErrorReturn(-1);
            }

            private void log(int n) {}
          }
          """
      )
    );
  }

  @Test
  void transformsRealWorldUserServiceExample() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import io.smallrye.mutiny.Uni;

          class UserService {
            Uni<User> createUser(String username) {
              return Uni.createFrom().item(new User(username))
                .onItem().transform(user -> enrichUser(user))
                .onItem().invoke(user -> auditLog("Creating user: " + user))
                .onFailure().retry().atMost(3)
                .onFailure().invoke(e -> logError("Failed to create user", e))
                .onFailure().recoverWithItem(new User("guest"));
            }

            Multi<User> listActiveUsers() {
              return Multi.createFrom().items(getUsers())
                .onItem().transform(user -> user.withStatus("active"))
                .onItem().invoke(user -> trackAccess(user))
                .onFailure().recoverWithItem(new User("error"));
            }

            private User enrichUser(User u) { return u; }
            private void auditLog(String msg) {}
            private void logError(String msg, Throwable e) {}
            private void trackAccess(User u) {}
            private User[] getUsers() { return new User[0]; }

            static class User {
              String name;
              User(String name) { this.name = name; }
              User withStatus(String status) { return this; }
              public String toString() { return name; }
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Flux;
          import reactor.core.publisher.Mono;

          class UserService {
            Mono<User> createUser(String username) {
              return Mono.just(new User(username)).map(user -> enrichUser(user)).doOnNext(user -> auditLog("Creating user: " + user)).retry(3).doOnError(e -> logError("Failed to create user", e)).onErrorReturn(new User("guest"));
            }

            Flux<User> listActiveUsers() {
              return Flux.just(getUsers()).map(user -> user.withStatus("active")).doOnNext(user -> trackAccess(user)).onErrorReturn(new User("error"));
            }

            private User enrichUser(User u) { return u; }
            private void auditLog(String msg) {}
            private void logError(String msg, Throwable e) {}
            private void trackAccess(User u) {}
            private User[] getUsers() { return new User[0]; }

            static class User {
              String name;
              User(String name) { this.name = name; }
              User withStatus(String status) { return this; }
              public String toString() { return name; }
            }
          }
          """
      )
    );
  }
}
