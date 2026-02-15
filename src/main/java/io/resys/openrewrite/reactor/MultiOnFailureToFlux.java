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
public class MultiOnFailureToFlux extends Recipe {

  @JsonCreator
  public MultiOnFailureToFlux() {
  }

  @Override
  public String getDisplayName() {
    return "Migrate Multi.onFailure() to Flux error handling";
  }

  @Override
  public String getDescription() {
    return "Migrates all Multi.onFailure() invocations to their Flux equivalents.";
  }

  @Override
  public TreeVisitor<?, ExecutionContext> getVisitor() {
    return new JavaIsoVisitor<>() {

      private final JavaTemplate onErrorReturnTemplate = JavaTemplate.builder("#{any()}.onErrorReturn(#{any()})")
        .build();

      private final JavaTemplate onErrorResumeFunctionTemplate = JavaTemplate.builder("#{any()}.onErrorResume(e -> reactor.core.publisher.Flux.just(#{any(java.util.function.Function)}.apply(e)))")
        .build();

      private final JavaTemplate onErrorResumeWithLambdaTemplate = JavaTemplate.builder("#{any()}.onErrorResume(e -> reactor.core.publisher.Flux.just(#{any()}))")
        .build();

      private final JavaTemplate onErrorResumeEmptyTemplate = JavaTemplate.builder("#{any()}.onErrorResume(e -> reactor.core.publisher.Flux.empty())")
        .build();

      private final JavaTemplate onErrorResumeTemplate = JavaTemplate.builder("#{any()}.onErrorResume(e -> #{any()})")
        .build();

      private final JavaTemplate doOnErrorTemplate = JavaTemplate.builder("#{any()}.doOnError(#{any(java.util.function.Consumer)})")
        .build();

      private final JavaTemplate retryTemplate = JavaTemplate.builder("#{any()}.retry(#{any(long)})")
        .build();

      private final JavaTemplate retryIndefinitelyTemplate = JavaTemplate.builder("#{any()}.retry()")
        .build();

      private final JavaTemplate retryBackoffTemplate = JavaTemplate.builder("#{any()}.retryWhen(reactor.util.retry.Retry.backoff(Long.MAX_VALUE, #{any(java.time.Duration)}).maxBackoff(#{any(java.time.Duration)}))")
        .build();

      @Override
      public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
        J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

        // Handle retry() chain methods: atMost(), indefinitely(), withBackOff()
        if (m.getSelect() instanceof J.MethodInvocation retryInvocation &&
            retryInvocation.getName().getSimpleName().equals("retry") &&
            retryInvocation.getSelect() instanceof J.MethodInvocation onFailureInvocation &&
            isOnFailureMethod(onFailureInvocation)) {

          Expression fluxExpression = getFluxExpression(onFailureInvocation);
          if (fluxExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          switch (methodName) {
            case "atMost" -> {
              if (!m.getArguments().isEmpty()) {
                Expression countArg = m.getArguments().get(0);
                m = retryTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, countArg);
              }
            }
            case "indefinitely" -> {
              m = retryIndefinitelyTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression);
            }
            case "withBackOff" -> {
              if (m.getArguments().size() >= 2) {
                Expression minArg = m.getArguments().get(0);
                Expression maxArg = m.getArguments().get(1);
                m = retryBackoffTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, minArg, maxArg);
              }
            }
          }
          return m;
        }

        // Handle onFailure() direct methods: recoverWithItem(), recoverWithNull(), recoverWithMulti(), invoke()
        if (m.getSelect() instanceof J.MethodInvocation onFailureInvocation &&
            isOnFailureMethod(onFailureInvocation)) {

          Expression fluxExpression = getFluxExpression(onFailureInvocation);
          if (fluxExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          switch (methodName) {
            case "recoverWithItem" -> {
              if (!m.getArguments().isEmpty()) {
                Expression arg = m.getArguments().get(0);
                // Check if the argument is an inline lambda
                if (arg instanceof J.Lambda lambda) {
                  // For inline lambdas, extract the body and wrap it in Flux.just()
                  org.openrewrite.java.tree.J lambdaBody = lambda.getBody();
                  if (lambdaBody instanceof Expression lambdaExpr) {
                    m = onErrorResumeWithLambdaTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, lambdaExpr);
                  } else {
                    // For lambda bodies that are blocks, use the function template instead
                    m = onErrorResumeFunctionTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, arg);
                  }
                } else if (isFunction(arg)) {
                  // For method references or function variables, use .apply(e)
                  m = onErrorResumeFunctionTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, arg);
                } else {
                  // Simple value
                  m = onErrorReturnTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, arg);
                }
              }
            }
            case "recoverWithNull" -> {
              m = onErrorResumeEmptyTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression);
            }
            case "recoverWithMulti" -> {
              if (!m.getArguments().isEmpty()) {
                Expression multiArg = m.getArguments().get(0);
                m = onErrorResumeTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, multiArg);
              }
            }
            case "invoke" -> {
              if (!m.getArguments().isEmpty()) {
                Expression consumerArg = m.getArguments().get(0);
                m = doOnErrorTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, consumerArg);
              }
            }
            case "retry" -> {
              // Don't transform here, wait for the next method in the chain
              return m;
            }
          }
        }

        return m;
      }

      private boolean isOnFailureMethod(J.MethodInvocation invocation) {
        // Check just the method name, as the type may have already been changed
        return invocation.getName().getSimpleName().equals("onFailure");
      }

      private Expression getFluxExpression(J.MethodInvocation onFailureInvocation) {
        // The flux expression is the select of the onFailure() call
        return onFailureInvocation.getSelect();
      }

      private boolean isFunction(Expression expr) {
        // Check if the expression is a lambda or method reference
        if (expr instanceof J.Lambda || expr instanceof J.MemberReference) {
          return true;
        }
        // Check if it's a variable with a function type
        if (expr instanceof J.Identifier id && id.getType() instanceof org.openrewrite.java.tree.JavaType.Parameterized param) {
          String typeName = param.getType().getFullyQualifiedName();
          return typeName.startsWith("java.util.function.");
        }
        return false;
      }
    };
  }
}
