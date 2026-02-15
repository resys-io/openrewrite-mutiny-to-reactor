# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an OpenRewrite recipe project for migrating SmallRye Mutiny reactive streams to Project Reactor. The project creates custom recipes that can automatically transform reactive code patterns from Mutiny (`Uni`) to Reactor (`Mono`).

## Technology Stack

- **Java 17** - Target JDK version
- **Maven** - Build tool and dependency management
- **OpenRewrite** - Code transformation framework
- **Lombok** - Optional annotation processor for recipe development
- **JUnit 5** - Testing framework
- **SmallRye Mutiny** - Source reactive library being migrated from
- **Project Reactor** - Target reactive library being migrated to

## Common Commands

### Build and Compilation
```bash
mvn compile                    # Compile main sources
mvn test-compile              # Compile test sources
mvn package                   # Build JAR artifact
```

### Testing
```bash
mvn test                      # Run all tests
mvn test -Dtest=SayHelloRecipeTest  # Run specific test class
mvn test -Dtest=SayHelloRecipeTest#addsHelloToFooBar  # Run specific test method
```

### Development
```bash
mvn clean                     # Clean build artifacts
mvn clean compile            # Clean and rebuild
mvn clean test              # Clean and run tests
```

## Architecture

### Chained Method Call Handling

Reactive APIs use chained method invocations like:
```java
Uni.createFrom().item("test").onFailure().recoverWithItem("fallback")
```

The rewrite recipes are designed to handle these chained calls correctly by:
1. Using `JavaIsoVisitor` to traverse the AST and process each method invocation independently
2. Preserving the chain structure while transforming only the relevant parts
3. Allowing multiple recipes to compose together (e.g., `UniToMono` + `UniOnFailureToMono`)

**See `CHAINED_CALLS.md` for detailed documentation on how chained method transformations work.**

### Recipe Structure
The project follows OpenRewrite's recipe pattern:

- **Main Recipe Classes** (`src/main/java/io/resys/rewrite/`): Contains the transformation logic
  - `MutinyToReactor.java` - **Master recipe** that combines all migrations (RECOMMENDED for complete migration)
  - `UniToMono.java` - Recipe that handles conversions from Uni.createFrom() to Mono
  - `UniOnItemToMono.java` - Recipe that handles conversions from Uni.onItem() to Mono operators
  - `UniOnFailureToMono.java` - Recipe that handles conversions from Uni.onFailure() to Mono error handling
  - `MultiToFlux.java` - Recipe that handles conversions from Multi.createFrom() to Flux
  - `MultiOnItemToFlux.java` - Recipe that handles conversions from Multi.onItem() to Flux operators
  - `MultiOnFailureToFlux.java` - Recipe that handles conversions from Multi.onFailure() to Flux error handling
  - Recipes extend `org.openrewrite.Recipe` and implement `getVisitor()` to return tree visitors
  - Uses `JavaIsoVisitor` to traverse and transform Java AST nodes
  - File `spec/UniToMono.md` contains how migration is done from Uni.createFrom() to Mono
  - File `spec/UniOnItemToMono.md` contains how migration is done from Uni.onItem() to Mono operators
  - File `spec/UniOnFailureToMono.md` contains how migration is done from Uni.onFailure() to Mono error handling
  - File `spec/MultiToFlux.md` contains how migration is done from Multi.createFrom() to Flux
  - File `spec/MultiOnItemToFlux.md` contains how migration is done from Multi.onItem() to Flux operators
  - File `spec/MultiOnFailureToFlux.md` contains how migration is done from Multi.onFailure() to Flux error handling
    
- **Test Classes** (`src/test/java/io/resys/rewrite/`): Contains recipe validation tests
  - `UniToMonoTest.java` - Tests for Uni.createFrom() transformations (10 tests)
  - `UniOnItemToMonoTest.java` - Tests for Uni.onItem() transformations (12 tests)
  - `UniOnFailureToMonoTest.java` - Tests for Uni.onFailure() transformations (11 tests)
  - `MultiToFluxTest.java` - Tests for Multi.createFrom() transformations (13 tests)
  - `MultiOnItemToFluxTest.java` - Tests for Multi.onItem() transformations (9 tests)
  - `MultiOnFailureToFluxTest.java` - Tests for Multi.onFailure() transformations (12 tests)
  - `ChainedTransformationsTest.java` - Tests for chained method invocations with single recipe (7 tests)
  - `CombinedRecipesTest.java` - Tests for multiple recipes working together (10 tests)
  - `MutinyToReactorTest.java` - Tests for complete migrations with complex chains (12 tests) ✨ NEW
  - All test classes implement `RewriteTest` interface
  - Uses `rewriteRun()` with before/after code samples to verify transformations
  - Tests verify both positive transformations and negative cases (no unwanted changes)
  - **Total: 96 tests, all passing** ✅

### Recipe Development Patterns

1. **Method Invocation Transformation**: The current recipe targets `io.smallrye.mutiny.groups.UniCreate.item()` calls for transformation
2. **AST Visitor Pattern**: Uses `visitMethodInvocation()` to identify and transform specific method calls
3. **Type-safe Transformations**: Checks declaring types and method names before applying transformations
4. **Immutable Recipes**: All recipes must be serializable and immutable (using `@Value` from Lombok)

### Configuration and Dependencies

The project includes OpenRewrite dependencies for:
- Java AST manipulation (`rewrite-java`, `rewrite-java-8/11/17`)
- Maven POM transformations (`rewrite-maven`)
- YAML/Properties/XML file transformations (additional rewrite modules)

Recipes can be configured with `@Option` parameters and must have JSON constructors for serialization.