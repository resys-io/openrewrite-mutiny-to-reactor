package io.resys.openrewrite.reactor;

import org.junit.jupiter.api.Test;
import org.openrewrite.test.RecipeSpec;
import org.openrewrite.test.RewriteTest;

import static org.openrewrite.java.Assertions.java;

class MultiOnItemToFluxTest implements RewriteTest {
  @Override
  public void defaults(RecipeSpec spec) {
    spec.recipe(new MultiOnItemToFlux())
      .typeValidationOptions(org.openrewrite.test.TypeValidation.none());
  }

  @Test
  void transformsOnItemTransform() {
    rewriteRun(
      java(
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onItem().transform(s -> s.toUpperCase());
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").map(s -> s.toUpperCase());
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

          import io.smallrye.mutiny.Multi;
          import io.smallrye.mutiny.Uni;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onItem().transformToUni(s -> Uni.createFrom().item(s.toUpperCase()));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import io.smallrye.mutiny.Uni;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").flatMap(s -> Uni.createFrom().item(s.toUpperCase()));
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

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b")
                .onItem().transformToMulti(s -> Multi.createFrom().items(s, s.toUpperCase()));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b").flatMap(s -> Multi.createFrom().items(s, s.toUpperCase()));
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

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onItem().ignore().andContinueWith("done");
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").ignoreElements().thenReturn("done");
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

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onItem().invoke(s -> System.out.println(s));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").doOnNext(s -> System.out.println(s));
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

          import io.smallrye.mutiny.Multi;
          import java.time.Duration;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onItem().delayIt().by(Duration.ofMillis(100));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;
          import java.time.Duration;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").delayElements(Duration.ofMillis(100));
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

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test1() {
              return Multi.createFrom().items("a", "b")
                .onItem().transform(s -> s.toUpperCase());
            }

            Multi<String> test2() {
              return Multi.createFrom().items("a", "b")
                .onItem().invoke(s -> System.out.println(s));
            }

            Multi<String> test3() {
              return Multi.createFrom().items("a", "b")
                .onItem().ignore().andContinueWith("done");
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test1() {
              return Multi.createFrom().items("a", "b").map(s -> s.toUpperCase());
            }

            Multi<String> test2() {
              return Multi.createFrom().items("a", "b").doOnNext(s -> System.out.println(s));
            }

            Multi<String> test3() {
              return Multi.createFrom().items("a", "b").ignoreElements().thenReturn("done");
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

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c")
                .onItem().transform(s -> s.toUpperCase())
                .onItem().invoke(s -> System.out.println(s));
            }
          }
          """,
        """
          package com.yourorg;

          import io.smallrye.mutiny.Multi;

          class FooBar {
            Multi<String> test() {
              return Multi.createFrom().items("a", "b", "c").map(s -> s.toUpperCase()).doOnNext(s -> System.out.println(s));
            }
          }
          """
      )
    );
  }
}
