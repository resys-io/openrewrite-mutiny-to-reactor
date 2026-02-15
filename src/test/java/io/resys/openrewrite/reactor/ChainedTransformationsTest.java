package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

/**
 * Tests to verify that chained method invocations are transformed correctly.
 * Reactive APIs commonly use method chaining like:
 * Uni.createFrom().item("test").onFailure().recoverWithItem("fallback")
 */
class ChainedTransformationsTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new UniOnFailureToMono())
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsChainedCreateFromWithOnFailureRecoverWithItem() {
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsChainedCreateFromWithOnFailureInvoke() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().invoke(e -> System.err.println(e));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").doOnError(e -> System.err.println(e));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsChainedCreateFromWithOnFailureRetry() {
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").retry(3);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsChainedCreateFromWithMultipleOnFailureCalls() {
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").doOnError(e -> System.err.println("Error: " + e)).onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsChainedFailureWithOnFailureRecoverWithUni() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().failure(new RuntimeException("error"))
                .onFailure().recoverWithUni(Uni.createFrom().item("recovered"));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().failure(new RuntimeException("error")).onErrorResume(e -> Uni.createFrom().item("recovered"));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsChainedWithLambdaRecoverWithItem() {
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").onErrorResume(e -> reactor.core.publisher.Mono.just("Error: " + e.getMessage()));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsLongChainWithMultipleOperators() {
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").retry(3).doOnError(e -> System.err.println(e)).onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }
}
