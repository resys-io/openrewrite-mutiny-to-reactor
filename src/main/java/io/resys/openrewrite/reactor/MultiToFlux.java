package io.resys.openrewrite.reactor;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.maven.AddDependency;

import java.util.Arrays;
import java.util.List;

// Making your recipe immutable helps make them idempotent and eliminates categories of possible bugs.
// Configuring your recipe in this way also guarantees that basic validation of parameters will be done for you by rewrite.
// Also note: All recipes must be serializable. This is verified by RewriteTest.rewriteRun() in your tests.
@Value
@EqualsAndHashCode(callSuper = false)
public class MultiToFlux extends Recipe {

  // All recipes must be serializable. This is verified by RewriteTest.rewriteRun() in your tests.
  @JsonCreator
  public MultiToFlux() {
  }

  @Override
  public String getDisplayName() {
    return "Migrate Multi.createFrom() to Flux";
  }

  @Override
  public String getDescription() {
    return "Migrates all Multi.createFrom() invocations to their Flux equivalents.";
  }

  @Override
  public List<Recipe> getRecipeList() {
    return Arrays.asList(
      new MultiCreateToFluxTransformer(),
      new ChangeType("io.smallrye.mutiny.Multi", "reactor.core.publisher.Flux", false),
      new AddDependency("io.projectreactor", "reactor-core", "3.7.9", null, null, null, null, null, null, null, null, null)
    );
  }

  public static class MultiCreateToFluxTransformer extends Recipe {
    @Override
    public String getDisplayName() {
      return "Transform Multi.createFrom() methods to Flux equivalents";
    }

    @Override
    public String getDescription() {
      return "Transforms all Multi.createFrom() method calls to their Flux equivalents.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
      return new JavaIsoVisitor<>() {

        private final JavaTemplate fluxJustTemplate = JavaTemplate.builder("Flux.just(#{any()})")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxFromIterableTemplate = JavaTemplate.builder("Flux.fromIterable(#{any(java.lang.Iterable)})")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxFromStreamTemplate = JavaTemplate.builder("Flux.fromStream(#{any(java.util.stream.Stream)})")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxRangeTemplate = JavaTemplate.builder("Flux.range(#{any(int)}, #{any(int)})")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxErrorTemplate = JavaTemplate.builder("Flux.error(#{any(java.lang.Throwable)})")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxEmptyTemplate = JavaTemplate.builder("Flux.empty()")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxNeverTemplate = JavaTemplate.builder("Flux.never()")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxFromTemplate = JavaTemplate.builder("Flux.from(#{any(org.reactivestreams.Publisher)})")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxIntervalTemplate = JavaTemplate.builder("Flux.interval(#{any(java.time.Duration)})")
          .imports("reactor.core.publisher.Flux")
          .build();

        private final JavaTemplate fluxCreateTemplate = JavaTemplate.builder("Flux.create(#{any(java.util.function.Consumer)})")
          .imports("reactor.core.publisher.Flux")
          .build();

        @Override
        public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
          J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

          String simpleName = m.getName().getSimpleName();
          // Check if this is a call on MultiCreate or if it's a createFrom() chain that got partially transformed
          if (m.getSelect() instanceof J.MethodInvocation select &&
            select.getMethodType() != null) {
            
            boolean isMultiCreateMethod = select.getMethodType().getDeclaringType().getFullyQualifiedName().equals("io.smallrye.mutiny.groups.MultiCreate");
            boolean isCreateFromChain = select.getName().getSimpleName().equals("createFrom") && 
              (select.getSelect() instanceof J.Identifier id && 
               (id.getSimpleName().equals("Multi") || id.getSimpleName().equals("Flux")));
            
            if (isMultiCreateMethod || isCreateFromChain) {

            switch (simpleName) {
              case "items" -> {
                // Multi.createFrom().items(1, 2, 3) -> Flux.just(1, 2, 3)
                // Multi.createFrom().items(stream) -> Flux.fromStream(stream)
                if (!m.getArguments().isEmpty()) {
                  // For items with multiple arguments, we need to handle them as varargs
                  if (m.getArguments().size() == 1) {
                    Expression itemArg = m.getArguments().get(0);
                    JavaType type = itemArg.getType();
                    if (type instanceof JavaType.Parameterized argType && "java.util.stream.Stream".equals(argType.getFullyQualifiedName())) {
                      Expression streamArg = m.getArguments().get(0);
                      m = fluxFromStreamTemplate.apply(getCursor(), m.getCoordinates().replace(), streamArg);
                      maybeAddImport("reactor.core.publisher.Flux");
                    } else {
                      m = fluxJustTemplate.apply(getCursor(), m.getCoordinates().replace(), itemArg);
                    }
                  } else {
                    // Multiple arguments - create template with all args
                    StringBuilder templateBuilder = new StringBuilder("Flux.just(");
                    for (int i = 0; i < m.getArguments().size(); i++) {
                      if (i > 0) templateBuilder.append(", ");
                      templateBuilder.append("#{any()}");
                    }
                    templateBuilder.append(")");
                    
                    JavaTemplate multiArgTemplate = JavaTemplate.builder(templateBuilder.toString())
                      .imports("reactor.core.publisher.Flux")
                      .build();
                    
                    m = multiArgTemplate.apply(getCursor(), m.getCoordinates().replace(), m.getArguments().toArray());
                  }
                  maybeAddImport("reactor.core.publisher.Flux");
                }
              }
              case "iterable" -> {
                // Multi.createFrom().iterable(list) -> Flux.fromIterable(list)
                if (!m.getArguments().isEmpty()) {
                  Expression iterableArg = m.getArguments().get(0);
                  m = fluxFromIterableTemplate.apply(getCursor(), m.getCoordinates().replace(), iterableArg);
                  maybeAddImport("reactor.core.publisher.Flux");
                }
              }
              case "stream" -> {
                // Multi.createFrom().stream(stream) -> Flux.fromStream(stream)
                if (!m.getArguments().isEmpty()) {
                  Expression streamArg = m.getArguments().get(0);
                  m = fluxFromStreamTemplate.apply(getCursor(), m.getCoordinates().replace(), streamArg);
                  maybeAddImport("reactor.core.publisher.Flux");
                }
              }
              case "range" -> {
                // Multi.createFrom().range(start, count) -> Flux.range(start, count)
                if (m.getArguments().size() >= 2) {
                  Expression startArg = m.getArguments().get(0);
                  Expression countArg = m.getArguments().get(1);
                  m = fluxRangeTemplate.apply(getCursor(), m.getCoordinates().replace(), startArg, countArg);
                  maybeAddImport("reactor.core.publisher.Flux");
                }
              }
              case "failure" -> {
                // Multi.createFrom().failure(e) -> Flux.error(e)
                if (!m.getArguments().isEmpty()) {
                  Expression errorArg = m.getArguments().get(0);
                  m = fluxErrorTemplate.apply(getCursor(), m.getCoordinates().replace(), errorArg);
                  maybeAddImport("reactor.core.publisher.Flux");
                }
              }
              case "empty" -> {
                // Multi.createFrom().empty() -> Flux.empty()
                m = fluxEmptyTemplate.apply(getCursor(), m.getCoordinates().replace());
                maybeAddImport("reactor.core.publisher.Flux");
              }
              case "nothing" -> {
                // Multi.createFrom().nothing() -> Flux.never()
                m = fluxNeverTemplate.apply(getCursor(), m.getCoordinates().replace());
                maybeAddImport("reactor.core.publisher.Flux");
              }
              case "publisher" -> {
                // Multi.createFrom().publisher(pub) -> Flux.from(pub)
                if (!m.getArguments().isEmpty()) {
                  Expression publisherArg = m.getArguments().get(0);
                  m = fluxFromTemplate.apply(getCursor(), m.getCoordinates().replace(), publisherArg);
                  maybeAddImport("reactor.core.publisher.Flux");
                }
              }
              case "emitter" -> {
                // Multi.createFrom().emitter(emitter -> {...}) -> Flux.create(sink -> {...})
                if (!m.getArguments().isEmpty()) {
                  Expression emitterArg = m.getArguments().get(0);
                  m = fluxCreateTemplate.apply(getCursor(), m.getCoordinates().replace(), emitterArg);
                  maybeAddImport("reactor.core.publisher.Flux");
                }
              }
            }
          }
          }
          
          // Handle Multi.createFrom().ticks().every(Duration) -> Flux.interval(Duration)
          if (m.getSelect() instanceof J.MethodInvocation select &&
              "every".equals(simpleName) &&
              m.getMethodType() != null &&
              select.getMethodType() != null) {
              
            boolean isMultiTickingMethod = select.getMethodType().getDeclaringType().getFullyQualifiedName().equals("io.smallrye.mutiny.groups.MultiCreateByTicking");
            boolean isTicksChain = select.getName().getSimpleName().equals("ticks") && 
              select.getSelect() instanceof J.MethodInvocation ticksSelect &&
              ticksSelect.getName().getSimpleName().equals("createFrom");
              
            if (isMultiTickingMethod || isTicksChain) {
              if (!m.getArguments().isEmpty()) {
                Expression durationArg = m.getArguments().get(0);
                m = fluxIntervalTemplate.apply(getCursor(), m.getCoordinates().replace(), durationArg);
                maybeAddImport("reactor.core.publisher.Flux");
              }
            }
          }

          return m;
        }
      };
    }
  }
}