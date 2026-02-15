package io.resys.openrewrite.reactor;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;

@Value
@EqualsAndHashCode(callSuper = false)
public class UniOnItemToMono extends Recipe {

  @JsonCreator
  public UniOnItemToMono() {
  }

  @Override
  public String getDisplayName() {
    return "Migrate Uni.onItem() to Mono operators";
  }

  @Override
  public String getDescription() {
    return "Migrates all Uni.onItem() invocations to their Mono equivalents.";
  }

  @Override
  public TreeVisitor<?, ExecutionContext> getVisitor() {
    return new JavaIsoVisitor<>() {

      private final JavaTemplate mapTemplate = JavaTemplate.builder("#{any()}.map(#{any(java.util.function.Function)})")
        .build();

      private final JavaTemplate flatMapTemplate = JavaTemplate.builder("#{any()}.flatMap(#{any(java.util.function.Function)})")
        .build();

      private final JavaTemplate flatMapManyTemplate = JavaTemplate.builder("#{any()}.flatMapMany(#{any(java.util.function.Function)})")
        .build();

      private final JavaTemplate thenMonoEmptyTemplate = JavaTemplate.builder("#{any()}.then(reactor.core.publisher.Mono.empty())")
        .build();

      private final JavaTemplate thenReturnTemplate = JavaTemplate.builder("#{any()}.thenReturn(#{any()})")
        .build();

      private final JavaTemplate doOnNextTemplate = JavaTemplate.builder("#{any()}.doOnNext(#{any(java.util.function.Consumer)})")
        .build();

      private final JavaTemplate delayElementTemplate = JavaTemplate.builder("#{any()}.delayElement(#{any(java.time.Duration)})")
        .build();

      private final JavaTemplate switchIfEmptyMonoJustTemplate = JavaTemplate.builder("#{any()}.switchIfEmpty(reactor.core.publisher.Mono.just(#{any()}))")
        .build();

      private final JavaTemplate switchIfEmptyMonoErrorTemplate = JavaTemplate.builder("#{any()}.switchIfEmpty(reactor.core.publisher.Mono.error(#{any(java.lang.Throwable)}))")
        .build();

      @Override
      public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
        J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

        // Handle delayIt().by() chain
        if (m.getSelect() instanceof J.MethodInvocation delayItInvocation &&
            delayItInvocation.getName().getSimpleName().equals("delayIt") &&
            delayItInvocation.getSelect() instanceof J.MethodInvocation onItemInvocation &&
            isOnItemMethod(onItemInvocation)) {

          Expression monoExpression = getMonoExpression(onItemInvocation);
          if (monoExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          if (methodName.equals("by")) {
            if (!m.getArguments().isEmpty()) {
              Expression durationArg = m.getArguments().get(0);
              m = delayElementTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, durationArg);
            }
          }
          return m;
        }

        // Handle ignore().andContinueWithNull() or ignore().andContinueWith(item) chain
        if (m.getSelect() instanceof J.MethodInvocation ignoreInvocation &&
            ignoreInvocation.getName().getSimpleName().equals("ignore") &&
            ignoreInvocation.getSelect() instanceof J.MethodInvocation onItemInvocation &&
            isOnItemMethod(onItemInvocation)) {

          Expression monoExpression = getMonoExpression(onItemInvocation);
          if (monoExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          switch (methodName) {
            case "andContinueWithNull" -> {
              m = thenMonoEmptyTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression);
            }
            case "andContinueWith" -> {
              if (!m.getArguments().isEmpty()) {
                Expression itemArg = m.getArguments().get(0);
                m = thenReturnTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, itemArg);
              }
            }
          }
          return m;
        }

        // Handle ifNull().continueWith() or ifNull().failWith() chain
        if (m.getSelect() instanceof J.MethodInvocation ifNullInvocation &&
            ifNullInvocation.getName().getSimpleName().equals("ifNull") &&
            ifNullInvocation.getSelect() instanceof J.MethodInvocation onItemInvocation &&
            isOnItemMethod(onItemInvocation)) {

          Expression monoExpression = getMonoExpression(onItemInvocation);
          if (monoExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          switch (methodName) {
            case "continueWith" -> {
              if (!m.getArguments().isEmpty()) {
                Expression itemArg = m.getArguments().get(0);
                m = switchIfEmptyMonoJustTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, itemArg);
              }
            }
            case "failWith" -> {
              if (!m.getArguments().isEmpty()) {
                Expression errorArg = m.getArguments().get(0);
                m = switchIfEmptyMonoErrorTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, errorArg);
              }
            }
          }
          return m;
        }

        // Handle onItem() direct methods: transform(), transformToUni(), transformToMulti(), invoke()
        if (m.getSelect() instanceof J.MethodInvocation onItemInvocation &&
            isOnItemMethod(onItemInvocation)) {

          Expression monoExpression = getMonoExpression(onItemInvocation);
          if (monoExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          switch (methodName) {
            case "transform" -> {
              if (!m.getArguments().isEmpty()) {
                Expression functionArg = m.getArguments().get(0);
                m = mapTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, functionArg);
              }
            }
            case "transformToUni" -> {
              if (!m.getArguments().isEmpty()) {
                Expression functionArg = m.getArguments().get(0);
                m = flatMapTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, functionArg);
              }
            }
            case "transformToMulti" -> {
              if (!m.getArguments().isEmpty()) {
                Expression functionArg = m.getArguments().get(0);
                m = flatMapManyTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, functionArg);
              }
            }
            case "invoke" -> {
              if (!m.getArguments().isEmpty()) {
                Expression consumerArg = m.getArguments().get(0);
                m = doOnNextTemplate.apply(getCursor(), m.getCoordinates().replace(), monoExpression, consumerArg);
              }
            }
            case "delayIt", "ignore", "ifNull" -> {
              // Don't transform here, wait for the next method in the chain
              return m;
            }
          }
        }

        return m;
      }

      private boolean isOnItemMethod(J.MethodInvocation invocation) {
        // Check just the method name, as the type may have already been changed
        return invocation.getName().getSimpleName().equals("onItem");
      }

      private Expression getMonoExpression(J.MethodInvocation onItemInvocation) {
        // The mono expression is the select of the onItem() call
        return onItemInvocation.getSelect();
      }
    };
  }
}
