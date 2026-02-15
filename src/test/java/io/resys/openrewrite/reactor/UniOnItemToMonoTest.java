package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class UniOnItemToMonoTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new UniOnItemToMono())
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsOnItemTransform() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onItem().transform(s -> s.toUpperCase());
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").map(s -> s.toUpperCase());
            }
          }
          """
      )
    );
  }

  @Test
  void transformsOnItemTransformToUni() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onItem().transformToUni(s -> Uni.createFrom().item(s.toUpperCase()));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").flatMap(s -> Uni.createFrom().item(s.toUpperCase()));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsOnItemTransformToMulti() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import io.smallrye.mutiny.Uni;

          class FooBar {
            Multi<String> test() {
              return Uni.createFrom().item("test")
                .onItem().transformToMulti(s -> Multi.createFrom().items(s, s.toUpperCase()));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import io.smallrye.mutiny.Uni;

          class FooBar {
            Multi<String> test() {
              return Uni.createFrom().item("test").flatMapMany(s -> Multi.createFrom().items(s, s.toUpperCase()));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsOnItemIgnoreAndContinueWithNull() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<Void> test() {
              return Uni.createFrom().item("test")
                .onItem().ignore().andContinueWithNull();
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<Void> test() {
              return Uni.createFrom().item("test").then(reactor.core.publisher.Mono.empty());
            }
          }
          """
      )
    );
  }

  @Test
  void transformsOnItemIgnoreAndContinueWith() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onItem().ignore().andContinueWith("done");
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").thenReturn("done");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsOnItemInvoke() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onItem().invoke(s -> System.out.println(s));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").doOnNext(s -> System.out.println(s));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsOnItemDelayItBy() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;
          import java.time.Duration;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onItem().delayIt().by(Duration.ofMillis(100));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;
          import java.time.Duration;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").delayElement(Duration.ofMillis(100));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsOnItemIfNullContinueWith() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onItem().ifNull().continueWith("default");
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").switchIfEmpty(reactor.core.publisher.Mono.just("default"));
            }
          }
          """
      )
    );
  }

  @Test
  void transformsOnItemIfNullFailWith() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onItem().ifNull().failWith(new IllegalStateException("null value"));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").switchIfEmpty(reactor.core.publisher.Mono.error(new IllegalStateException("null value")));
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
  void transformsMultipleOnItemCalls() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test1() {
              return Uni.createFrom().item("test")
                .onItem().transform(s -> s.toUpperCase());
            }

            Uni<String> test2() {
              return Uni.createFrom().item("test")
                .onItem().invoke(s -> System.out.println(s));
            }

            Uni<String> test3() {
              return Uni.createFrom().item("test")
                .onItem().ignore().andContinueWith("done");
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test1() {
              return Uni.createFrom().item("test").map(s -> s.toUpperCase());
            }

            Uni<String> test2() {
              return Uni.createFrom().item("test").doOnNext(s -> System.out.println(s));
            }

            Uni<String> test3() {
              return Uni.createFrom().item("test").thenReturn("done");
            }
          }
          """
      )
    );
  }

  @Test
  void transformsChainedOnItemOperations() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test")
                .onItem().transform(s -> s.toUpperCase())
                .onItem().invoke(s -> System.out.println(s));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Uni;

          class FooBar {
            Uni<String> test() {
              return Uni.createFrom().item("test").map(s -> s.toUpperCase()).doOnNext(s -> System.out.println(s));
            }
          }
          """
      )
    );
  }
}
