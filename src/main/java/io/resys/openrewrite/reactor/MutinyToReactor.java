package io.resys.openrewrite.reactor;

import com.fasterxml.jackson.annotation.JsonCreator;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.openrewrite.Recipe;
import org.openrewrite.java.ChangeType;
import org.openrewrite.maven.AddDependency;

import java.util.Arrays;
import java.util.List;

/**
 * Master recipe that combines all SmallRye Mutiny to Project Reactor migration recipes.
 * This recipe applies all transformations in the correct order to handle:
 * - Uni creation, item operators, and error handling
 * - Multi creation, item operators, and error handling
 * - Type changes from Uni/Multi to Mono/Flux
 * - Dependency management
 */
@Value
@EqualsAndHashCode(callSuper = false)
public class MutinyToReactor extends Recipe {

  @JsonCreator
  public MutinyToReactor() {
  }

  @Override
  public String getDisplayName() {
    return "Migrate SmallRye Mutiny to Project Reactor";
  }

  @Override
  public String getDescription() {
    return "Comprehensive migration from SmallRye Mutiny (Uni/Multi) to Project Reactor (Mono/Flux). " +
           "Transforms all creation methods, item operators, and error handling patterns.";
  }

  @Override
  public List<Recipe> getRecipeList() {
    return Arrays.asList(
      // Uni transformations
      new UniToMono.UniCreateToMonoTransformer(),
      new UniOnItemToMono(),
      new UniOnFailureToMono(),

      // Multi transformations
      new MultiToFlux.MultiCreateToFluxTransformer(),
      new MultiOnItemToFlux(),
      new MultiOnFailureToFlux(),

      // Type changes (Uni -> Mono, Multi -> Flux)
      new ChangeType("io.smallrye.mutiny.Uni", "reactor.core.publisher.Mono", false),
      new ChangeType("io.smallrye.mutiny.Multi", "reactor.core.publisher.Flux", false),

      // Add Reactor dependency
      new AddDependency("io.projectreactor", "reactor-core", "3.7.9", null, null, null, null, null, null, null, null, null)
    );
  }
}
