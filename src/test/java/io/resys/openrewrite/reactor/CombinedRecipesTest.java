package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

/**
 * Tests to verify that multiple recipes work together correctly when applied in sequence.
 * This simulates a real migration scenario where both UniToMono and UniOnFailureToMono
 * recipes are applied together.
 */
class CombinedRecipesTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    // Apply both recipes in the order they would be applied in a real migration
    spec.recipes(
        new UniToMono(),
        new UniOnFailureToMono()
      )
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsUniCreateFromItemWithOnFailureRecoverWithItem() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().recoverWithItem("fallback");
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.just("test").onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromItemWithOnFailureRetry() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().retry().atMost(3);
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.just("test").retry(3);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromFailureWithOnFailureRecoverWithNull() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().failure(new RuntimeException("error"))
                .onFailure().recoverWithNull();
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.error(new RuntimeException("error")).onErrorResume(e -> reactor.core.publisher.Mono.empty());
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromItemWithMultipleOnFailureOperations() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().invoke(e -> System.err.println("Error: " + e))
                .onFailure().recoverWithItem("fallback");
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.just("test").doOnError(e -> System.err.println("Error: " + e)).onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsComplexChainWithCreateFromAndOnFailure() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().retry().atMost(3)
                .onFailure().invoke(e -> System.err.println(e))
                .onFailure().recoverWithItem("fallback");
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.just("test").retry(3).doOnError(e -> System.err.println(e)).onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromOptionalWithOnFailureRecoverWithItem() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;
          import java.util.Optional;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().optional(Optional.of("test"))
                .onFailure().recoverWithItem("fallback");
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          import java.util.Optional;

          class FooBar {
            Mono<String> test() {
              return Mono.justOrEmpty(Optional.of("test")).onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromItemWithOnFailureLambdaRecoverWithItem() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().recoverWithItem(e -> "Error: " + e.getMessage());
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test() {
              return Mono.just("test").onErrorResume(e -> reactor.core.publisher.Mono.just("Error: " + e.getMessage()));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromItemWithOnFailureRecoverWithUni() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test(Uni<String> fallbackUni) {
              return Uni.createFrom().item("test")
                .onFailure().recoverWithUni(fallbackUni);
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test(Mono<String> fallbackUni) {
              return Mono.just("test").onErrorResume(e -> fallbackUni);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsUniCreateFromItemWithRetryBackoff() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;
          import java.time.Duration;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(5));
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          import java.time.Duration;

          class FooBar {
            Mono<String> test() {
              return Mono.just("test").retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100)).maxBackoff(Duration.ofSeconds(5)));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultipleUniInstancesWithDifferentChains() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test1() {
              return Uni.createFrom().item("test1")
                .onFailure().recoverWithItem("fallback1");
            }

            Uni<Integer> test2() {
              return Uni.createFrom().item(42)
                .onFailure().retry().atMost(3);
            }

            Uni<String> test3() {
              return Uni.createFrom().failure(new RuntimeException())
                .onFailure().invoke(e -> System.err.println(e))
                .onFailure().recoverWithItem("fallback3");
            }
          }
          """,
        """
          package com.yourorg;

          import reactor.core.publisher.Mono;

          class FooBar {
            Mono<String> test1() {
              return Mono.just("test1").onErrorReturn("fallback1");
            }

            Mono<Integer> test2() {
              return Mono.just(42).retry(3);
            }

            Mono<String> test3() {
              return Mono.error(new RuntimeException()).doOnError(e -> System.err.println(e)).onErrorReturn("fallback3");
            }
          }
          """
      )
    );
  }
}
