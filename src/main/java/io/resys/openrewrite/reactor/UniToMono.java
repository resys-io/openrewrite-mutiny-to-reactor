package io.resys.openrewrite.reactor;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.openrewrite.ExecutionContext;
import org.openrewrite.Recipe;
import org.openrewrite.Tree;
import org.openrewrite.TreeVisitor;
import org.openrewrite.java.ChangeType;
import org.openrewrite.java.JavaIsoVisitor;
import org.openrewrite.java.JavaTemplate;
import org.openrewrite.java.tree.Expression;
import org.openrewrite.java.tree.J;
import org.openrewrite.java.tree.JavaType;
import org.openrewrite.java.tree.Space;
import org.openrewrite.marker.Markers;
import org.openrewrite.maven.AddDependency;

import java.util.Arrays;
import java.util.List;

// Making your recipe immutable helps make them idempotent and eliminates categories of possible bugs.
// Configuring your recipe in this way also guarantees that basic validation of parameters will be done for you by rewrite.
// Also note: All recipes must be serializable. This is verified by RewriteTest.rewriteRun() in your tests.
@Value
@EqualsAndHashCode(callSuper = false)
public class UniToMono extends Recipe {

  public static final J.Literal NULL_EXPR = new J.Literal(Tree.randomId(), Space.EMPTY, Markers.EMPTY, null, "null", null, JavaType.Primitive.Null);

  // All recipes must be serializable. This is verified by RewriteTest.rewriteRun() in your tests.
  @JsonCreator
  public UniToMono() {
  }

  @Override
  public String getDisplayName() {
    return "Migrate Uni.createFrom() to Mono";
  }

  @Override
  public String getDescription() {
    return "Migrates all Uni.createFrom() invocations to their Mono equivalents.";
  }

  @Override
  public List<Recipe> getRecipeList() {
    return Arrays.asList(
      new UniCreateToMonoTransformer(),
      new ChangeType("io.smallrye.mutiny.Uni", "reactor.core.publisher.Mono", false),
      new AddDependency("org.reactivestreams", "reactor-core", "1.0.3", null, null, null, null, null, null, null, null, null)
    );
  }

  public static class UniCreateToMonoTransformer extends Recipe {
    @Override
    public String getDisplayName() {
      return "Transform Uni.createFrom() methods to Mono equivalents";
    }

    @Override
    public String getDescription() {
      return "Transforms all Uni.createFrom() method calls to their Mono equivalents.";
    }

    @Override
    public TreeVisitor<?, ExecutionContext> getVisitor() {
      return new JavaIsoVisitor<>() {

        private final JavaTemplate monoJustTemplate = JavaTemplate.builder("Mono.just(#{any(java.lang.Object)})")
          .imports("reactor.core.publisher.Mono")
          .build();

        private final JavaTemplate monoEmptyTemplate = JavaTemplate.builder("Mono.empty()")
          .imports("reactor.core.publisher.Mono")
          .build();
        
        private final JavaTemplate monoErrorTemplate = JavaTemplate.builder("Mono.error(#{any(java.lang.Throwable)})")
          .imports("reactor.core.publisher.Mono")
          .build();
        
        private final JavaTemplate monoNeverTemplate = JavaTemplate.builder("Mono.never()")
          .imports("reactor.core.publisher.Mono")
          .build();
        
        private final JavaTemplate monoJustOrEmptyTemplate = JavaTemplate.builder("Mono.justOrEmpty(#{any(java.util.Optional)})")
          .imports("reactor.core.publisher.Mono")
          .build();
        
        private final JavaTemplate monoCreateTemplate = JavaTemplate.builder("Mono.create(#{any(java.util.function.Consumer)})")
          .imports("reactor.core.publisher.Mono")
          .build();
        
        private final JavaTemplate monoFromTemplate = JavaTemplate.builder("Mono.from(#{any(org.reactivestreams.Publisher)})")
          .imports("reactor.core.publisher.Mono")
          .build();

        @Override
        public J.MethodInvocation visitMethodInvocation(J.MethodInvocation method, ExecutionContext ctx) {
          J.MethodInvocation m = super.visitMethodInvocation(method, ctx);

          String simpleName = m.getName().getSimpleName();
          // Check if this is a call on UniCreate or if it's a createFrom() chain that got partially transformed
          if (m.getSelect() instanceof J.MethodInvocation select &&
            select.getMethodType() != null) {
            
            boolean isUniCreateMethod = select.getMethodType().getDeclaringType().getFullyQualifiedName().equals("io.smallrye.mutiny.groups.UniCreate");
            boolean isCreateFromChain = select.getName().getSimpleName().equals("createFrom") && 
              (select.getSelect() instanceof J.Identifier id && 
               (id.getSimpleName().equals("Uni") || id.getSimpleName().equals("Mono")));
            
            if (isUniCreateMethod || isCreateFromChain) {
            
            switch (simpleName) {
              case "item" -> {
                // Uni.createFrom().item(x) -> Mono.just(x)
                if (!m.getArguments().isEmpty()) {
                  Expression itemArg = m.getArguments().get(0);
                  m = monoJustTemplate.apply(getCursor(), m.getCoordinates().replace(), itemArg);
                  maybeAddImport("reactor.core.publisher.Mono");
                }
              }
              case "nullItem" -> {
                // Uni.createFrom().nullItem() -> Mono.empty() (since Mono.just(null) is invalid)
                m = monoEmptyTemplate.apply(getCursor(), m.getCoordinates().replace());
                maybeAddImport("reactor.core.publisher.Mono");
              }
              case "voidItem" -> {
                // Uni.createFrom().voidItem() -> Mono.empty()
                m = monoEmptyTemplate.apply(getCursor(), m.getCoordinates().replace());
                maybeAddImport("reactor.core.publisher.Mono");
              }
              case "failure" -> {
                // Uni.createFrom().failure(e) -> Mono.error(e)
                if (!m.getArguments().isEmpty()) {
                  Expression errorArg = m.getArguments().get(0);
                  m = monoErrorTemplate.apply(getCursor(), m.getCoordinates().replace(), errorArg);
                  maybeAddImport("reactor.core.publisher.Mono");
                }
              }
              case "nothing" -> {
                // Uni.createFrom().nothing() -> Mono.never()
                m = monoNeverTemplate.apply(getCursor(), m.getCoordinates().replace());
                maybeAddImport("reactor.core.publisher.Mono");
              }
              case "optional" -> {
                // Uni.createFrom().optional(opt) -> Mono.justOrEmpty(opt)
                if (!m.getArguments().isEmpty()) {
                  Expression optionalArg = m.getArguments().get(0);
                  m = monoJustOrEmptyTemplate.apply(getCursor(), m.getCoordinates().replace(), optionalArg);
                  maybeAddImport("reactor.core.publisher.Mono");
                }
              }
              case "emitter" -> {
                // Uni.createFrom().emitter(emitter -> {...}) -> Mono.create(sink -> {...})
                if (!m.getArguments().isEmpty()) {
                  Expression emitterArg = m.getArguments().get(0);
                  m = monoCreateTemplate.apply(getCursor(), m.getCoordinates().replace(), emitterArg);
                  maybeAddImport("reactor.core.publisher.Mono");
                }
              }
              case "publisher" -> {
                // Uni.createFrom().publisher(publisher) -> Mono.from(publisher)
                if (!m.getArguments().isEmpty()) {
                  Expression publisherArg = m.getArguments().get(0);
                  m = monoFromTemplate.apply(getCursor(), m.getCoordinates().replace(), publisherArg);
                  maybeAddImport("reactor.core.publisher.Mono");
                }
              }
            }
          }
          }

          return m;
        }
      };
    }
  }
}