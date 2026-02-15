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
public class MultiOnItemToFlux extends Recipe {

  @JsonCreator
  public MultiOnItemToFlux() {
  }

  @Override
  public String getDisplayName() {
    return "Migrate Multi.onItem() to Flux operators";
  }

  @Override
  public String getDescription() {
    return "Migrates all Multi.onItem() invocations to their Flux equivalents.";
  }

  @Override
  public TreeVisitor<?, ExecutionContext> getVisitor() {
    return new JavaIsoVisitor<>() {

      private final JavaTemplate mapTemplate = JavaTemplate.builder("#{any()}.map(#{any(java.util.function.Function)})")
        .build();

      private final JavaTemplate flatMapMonoTemplate = JavaTemplate.builder("#{any()}.flatMap(#{any(java.util.function.Function)})")
        .build();

      private final JavaTemplate flatMapFluxTemplate = JavaTemplate.builder("#{any()}.flatMap(#{any(java.util.function.Function)})")
        .build();

      private final JavaTemplate ignoreElementsThenReturnTemplate = JavaTemplate.builder("#{any()}.ignoreElements().thenReturn(#{any()})")
        .build();

      private final JavaTemplate doOnNextTemplate = JavaTemplate.builder("#{any()}.doOnNext(#{any(java.util.function.Consumer)})")
        .build();

      private final JavaTemplate delayElementsTemplate = JavaTemplate.builder("#{any()}.delayElements(#{any(java.time.Duration)})")
        .build();

      @Override
      public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
        J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

        // Handle delayIt().by() chain
        if (m.getSelect() instanceof J.MethodInvocation delayItInvocation &&
            delayItInvocation.getName().getSimpleName().equals("delayIt") &&
            delayItInvocation.getSelect() instanceof J.MethodInvocation onItemInvocation &&
            isOnItemMethod(onItemInvocation)) {

          Expression fluxExpression = getFluxExpression(onItemInvocation);
          if (fluxExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          if (methodName.equals("by")) {
            if (!m.getArguments().isEmpty()) {
              Expression durationArg = m.getArguments().get(0);
              m = delayElementsTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, durationArg);
            }
          }
          return m;
        }

        // Handle ignore().andContinueWith(item) chain
        // Note: andContinueWithNull() is not directly supported in Reactor
        if (m.getSelect() instanceof J.MethodInvocation ignoreInvocation &&
            ignoreInvocation.getName().getSimpleName().equals("ignore") &&
            ignoreInvocation.getSelect() instanceof J.MethodInvocation onItemInvocation &&
            isOnItemMethod(onItemInvocation)) {

          Expression fluxExpression = getFluxExpression(onItemInvocation);
          if (fluxExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          if (methodName.equals("andContinueWith")) {
            if (!m.getArguments().isEmpty()) {
              Expression itemArg = m.getArguments().get(0);
              m = ignoreElementsThenReturnTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, itemArg);
            }
          }
          // Note: andContinueWithNull() should be handled with a warning or left as-is
          return m;
        }

        // Handle onItem() direct methods: transform(), transformToUni(), transformToMulti(), invoke()
        if (m.getSelect() instanceof J.MethodInvocation onItemInvocation &&
            isOnItemMethod(onItemInvocation)) {

          Expression fluxExpression = getFluxExpression(onItemInvocation);
          if (fluxExpression == null) {
            return m;
          }

          String methodName = m.getName().getSimpleName();
          switch (methodName) {
            case "transform" -> {
              if (!m.getArguments().isEmpty()) {
                Expression functionArg = m.getArguments().get(0);
                m = mapTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, functionArg);
              }
            }
            case "transformToUni" -> {
              if (!m.getArguments().isEmpty()) {
                Expression functionArg = m.getArguments().get(0);
                m = flatMapMonoTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, functionArg);
              }
            }
            case "transformToMulti" -> {
              if (!m.getArguments().isEmpty()) {
                Expression functionArg = m.getArguments().get(0);
                m = flatMapFluxTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, functionArg);
              }
            }
            case "invoke" -> {
              if (!m.getArguments().isEmpty()) {
                Expression consumerArg = m.getArguments().get(0);
                m = doOnNextTemplate.apply(getCursor(), m.getCoordinates().replace(), fluxExpression, consumerArg);
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

      private Expression getFluxExpression(J.MethodInvocation onItemInvocation) {
        // The flux expression is the select of the onItem() call
        return onItemInvocation.getSelect();
      }
    };
  }
}
