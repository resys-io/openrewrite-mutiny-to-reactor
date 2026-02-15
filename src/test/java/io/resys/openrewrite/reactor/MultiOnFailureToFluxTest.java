package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class MultiOnFailureToFluxTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new MultiOnFailureToFlux())
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsRecoverWithItemValue() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().recoverWithItem("fallback");
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsRecoverWithItemFunction() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().recoverWithItem(e -> "error: " + e.getMessage());
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").onErrorResume(e -> reactor.core.publisher.Flux.just("error: " + e.getMessage()));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsRecoverWithNull() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().failure(new RuntimeException())
                .onFailure().recoverWithNull();
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().failure(new RuntimeException()).onErrorResume(e -> reactor.core.publisher.Flux.empty());
            }
          }
          """
      )
    );
  }

  @Test
  void transformsRecoverWithMulti() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test(Multi<String> fallbackMulti) {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().recoverWithMulti(fallbackMulti);
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test(Multi<String> fallbackMulti) {
              return Multi.createFrom().items("a", "b", "c").onErrorResume(e -> fallbackMulti);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsInvoke() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().invoke(e -> System.err.println("Error: " + e));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").doOnError(e -> System.err.println("Error: " + e));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsRetryAtMost() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().retry().atMost(3);
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").retry(3);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsRetryIndefinitely() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().retry().indefinitely();
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").retry();
            }
          }
          """
      )
    );
  }

  @Test
  void transformsRetryWithBackOff() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import java.time.Duration;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().retry().withBackOff(Duration.ofMillis(100), Duration.ofSeconds(5));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import java.time.Duration;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100)).maxBackoff(Duration.ofSeconds(5)));
            }
          }
          """
      )
    );
  }

  @Test
  void doesNotChangeOtherMethodCalls() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          class FooBar {
            public String hello() {
              return "";
            }
          }
          """
      )
    );
  }

  @Test
  void transformsMultipleOnFailureCalls() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test1() {
              return Multi.createFrom().items("a", "b")
                .onFailure().recoverWithItem("fallback");
            }

            Multi<Integer> test2() {
              return Multi.createFrom().items(1, 2, 3)
                .onFailure().invoke(e -> System.err.println(e));
            }

            Multi<String> test3() {
              return Multi.createFrom().items("a", "b")
                .onFailure().retry().atMost(5);
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test1() {
              return Multi.createFrom().items("a", "b").onErrorReturn("fallback");
            }

            Multi<Integer> test2() {
              return Multi.createFrom().items(1, 2, 3).doOnError(e -> System.err.println(e));
            }

            Multi<String> test3() {
              return Multi.createFrom().items("a", "b").retry(5);
            }
          }
          """
      )
    );
  }

  @Test
  void transformsRecoverWithItemMethodReference() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            private String handleError(Throwable e) {
              return "handled";
            }

            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().recoverWithItem(this::handleError);
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            private String handleError(Throwable e) {
              return "handled";
            }

            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").onErrorResume(e -> reactor.core.publisher.Flux.just(this::handleError.apply(e)));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsChainedOnFailureOperations() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onFailure().invoke(e -> System.err.println("Error: " + e))
                .onFailure().recoverWithItem("fallback");
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").doOnError(e -> System.err.println("Error: " + e)).onErrorReturn("fallback");
            }
          }
          """
      )
    );
  }
}
