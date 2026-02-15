package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class UniOnFailureToMonoTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new UniOnFailureToMono())
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsRecoverWithItemValue() {
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
  void transformsRecoverWithItemFunction() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().recoverWithItem(e -> "error: " + e.getMessage());
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").onErrorResume(e -> reactor.core.publisher.Mono.just("error: " + e.getMessage()));
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().failure(new RuntimeException())
                .onFailure().recoverWithNull();
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().failure(new RuntimeException()).onErrorResume(e -> reactor.core.publisher.Mono.empty());
            }
          }
          """
      )
    );
  }

  @Test
  void transformsRecoverWithUni() {
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test(Uni<String> fallbackUni) {
              return Uni.createFrom().item("test").onErrorResume(e -> fallbackUni);
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().invoke(e -> System.err.println("Error: " + e));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").doOnError(e -> System.err.println("Error: " + e));
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
  void transformsRetryIndefinitely() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().retry().indefinitely();
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").retry();
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

          import io.smallrye.mutiny.Uni;
          import java.time.Duration;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, Duration.ofMillis(100)).maxBackoff(Duration.ofSeconds(5)));
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test1() {
              return Uni.createFrom().item("test")
                .onFailure().recoverWithItem("fallback");
            }

            Uni<Integer> test2() {
              return Uni.createFrom().item(42)
                .onFailure().invoke(e -> System.err.println(e));
            }

            Uni<String> test3() {
              return Uni.createFrom().item("test")
                .onFailure().retry().atMost(5);
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test1() {
              return Uni.createFrom().item("test").onErrorReturn("fallback");
            }

            Uni<Integer> test2() {
              return Uni.createFrom().item(42).doOnError(e -> System.err.println(e));
            }

            Uni<String> test3() {
              return Uni.createFrom().item("test").retry(5);
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

          import io.smallrye.mutiny.Uni;

          class FooBar {
            private String handleError(Throwable e) {
              return "handled";
            }

            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onFailure().recoverWithItem(this::handleError);
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            private String handleError(Throwable e) {
              return "handled";
            }

            Uni<String> test() {
              return Uni.createFrom().item("test").onErrorResume(e -> reactor.core.publisher.Mono.just(this::handleError.apply(e)));
            }
          }
          """
      )
    );
  }
}
