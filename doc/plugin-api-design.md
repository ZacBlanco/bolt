# Bolt Pluggable Architecture: API and File Layout (Draft v1)

## Goals

- Make all major engine extension points loadable at runtime.
- Treat existing Bolt built-ins as first-party plugins.
- Keep plugin API stable while allowing engine internals to evolve.
- Support these plugin categories:
  - Functions (UDF/UDAF)
  - Operators (new execution nodes)
  - Optimizers (rule/cost passes)
  - Logical types
  - Vector formats / data layouts
  - Connectors / storage engines

## Design Principles

- Use a **single plugin manager** and **single entrypoint ABI**.
- Keep plugin-to-host surface **small and explicit**.
- Keep domain-specific registration in dedicated registries (functions, connectors, etc.).
- Separate:
  - **Public plugin API** (stable)
  - **Internal adapters** (map plugin API to current Bolt registries)
  - **Built-in plugins** (current Bolt implementation re-expressed as plugins)

## Proposed API Shape

### 1. Shared Library Entrypoint

Every plugin `.so` exports:

```cpp
extern "C" bool boltPluginInitV1(bytedance::bolt::plugin::PluginRegistrar&);
extern "C" void boltPluginShutdownV1();
```

- `boltPluginInitV1` registers capabilities into the host.
- `boltPluginShutdownV1` unregisters/cleans plugin-owned state.

### 2. Plugin Registrar Interface

```cpp
namespace bytedance::bolt::plugin {

struct PluginMetadata {
  std::string name;          // e.g. "builtin.prestosql.functions"
  std::string version;       // plugin version
  std::string boltAbi;       // host ABI expected, e.g. "bolt-plugin-v1"
  std::string vendor;
};

struct ScalarFunctionSpec {
  std::string name;
  std::vector<FunctionSignaturePtr> signatures;
  exec::VectorFunctionFactory factory;
  exec::VectorFunctionMetadata metadata;
  bool overwrite{true};
};

struct AggregateFunctionSpec {
  std::string name;
  std::vector<exec::AggregateFunctionSignaturePtr> signatures;
  exec::AggregateFunctionFactory factory;
  bool registerCompanionFunctions{false};
  bool overwrite{false};
};

struct LogicalTypeSpec {
  std::string name;
  std::unique_ptr<const CustomTypeFactories> factories;
};

struct ConnectorFactorySpec {
  std::string connectorName;
  std::shared_ptr<connector::ConnectorFactory> factory;
};

struct OperatorSpec {
  std::string name;
  // Public API calls this an "Operator". Internally Bolt still maps
  // PlanNode -> Operator via PlanNodeTranslator.
  std::unique_ptr<exec::Operator::PlanNodeTranslator> translator;
};

class OptimizerPass {
 public:
  virtual ~OptimizerPass() = default;
  virtual core::PlanNodePtr transform(
      const core::PlanNodePtr& inputPlan,
      const OptimizerContext& context) = 0;
};

enum class OptimizerStage {
  kLogical,
  kPhysical,
  kPostPhysical,
};

struct OptimizerContext {
  // Reserved for config, stats, catalog handles, cost model hooks, etc.
};

struct OptimizerPassSpec {
  std::string name;
  OptimizerStage stage;
  std::shared_ptr<OptimizerPass> pass;
};

struct VectorFormatSpec {
  VectorSerde::Kind kind;
  std::unique_ptr<VectorSerde> serde;
};

class PluginRegistrar {
 public:
  virtual ~PluginRegistrar() = default;

  // Default no-op so plugins can register only the subsets they need.
  virtual void setMetadata(PluginMetadata metadata) {}

  // 1) Functions
  virtual void addScalarFunction(ScalarFunctionSpec spec) {}
  virtual void addAggregateFunction(AggregateFunctionSpec spec) {}

  // 2) Operators
  virtual void addOperator(OperatorSpec spec) {}

  // 3) Optimizers
  virtual void addOptimizerPass(OptimizerPassSpec spec) {}

  // 4) Logical types
  virtual void addLogicalType(LogicalTypeSpec spec) {}

  // 5) Vector formats / data layouts
  virtual void addVectorFormat(VectorFormatSpec spec) {}

  // 6) Connectors / storage engines
  virtual void addConnectorFactory(ConnectorFactorySpec spec) {}
};

} // namespace bytedance::bolt::plugin
```

Notes:

- v1 API is strict by category: functions, connectors, operators, types, optimizers, vector formats register through separate typed methods.
- Host maps these specs to existing Bolt registries internally (`registerFunction`, `registerConnectorFactory`, `Operator::registerOperator`, `registerCustomType`, `registerNamedVectorSerde`, etc.).
- This removes generic set-level callbacks and makes intent explicit in the public API.
- Function specs use Bolt-native factory types:
  - scalar/vector: `exec::VectorFunctionFactory`
  - aggregate: `exec::AggregateFunctionFactory`
- Optimizer stages are enum-typed (`OptimizerStage`) to avoid ad-hoc string matching and ordering ambiguity.
- Optimizer passes are pure transforms: `transform(inputPlan, context) -> outputPlan`.

### Function Registration Internals (Required Change)

Plugin registrar can install function specs via existing Bolt registry APIs:

```cpp
exec::registerStatefulVectorFunction(
    spec.name, spec.signatures, spec.factory, spec.metadata, spec.overwrite);

exec::registerAggregateFunction(
    spec.name,
    spec.signatures,
    spec.factory,
    spec.registerCompanionFunctions,
    spec.overwrite);
```

### Why This Is Stricter

- Accidental misuse is reduced because plugin code calls explicit methods (`addScalarFunction`, `addLogicalType`, `addConnectorFactory`, `addOperator`) with typed specs.
- Host can validate each spec (name collisions, duplicate aliases, missing metadata) before installation.
- Per-category ownership is tracked automatically, enabling targeted unload where supported.
- Function registration call sites are centralized in registrar internals (plugins do not call `registerFunction` directly).

Security/containment caveat:

- A shared library can still execute arbitrary code in `boltPluginInitV1`.
- This API enforces structure and validation for normal plugins, but cannot fully sandbox malicious code by itself.

### Optimizer API Rationale

Why `transform(...) -> new plan` is the v1 direction:

- deterministic composition: pass output becomes explicit input to next pass.
- easier correctness and testing: no hidden in-place mutations.
- safer rollback/debugging: host can retain previous plan snapshots.
- better future concurrency options: immutable-style plan flow is easier to parallelize/analyze.

Tradeoff:

- can increase allocation/copy cost if full plan cloning is naive.
- mitigation is structural sharing (reuse unchanged subtrees) so only modified branches are rebuilt.

### Concrete User Plugin Example

Below is what a user-authored plugin looks like with strict typed registration.

```cpp
#include "bolt/plugin/api/PluginRegistrar.h"

#include "bolt/connectors/Connector.h"
#include "bolt/expression/VectorFunction.h"
#include "bolt/type/Type.h"

using namespace bytedance::bolt;

namespace {

std::vector<FunctionSignaturePtr> acmeAddOneSignatures();
exec::VectorFunctionFactory acmeAddOneFactory();

// Example custom type factory.
class TinyTagFactories final : public CustomTypeFactories {
 public:
  TypePtr getType() const override {
    return VARCHAR();
  }

  exec::CastOperatorPtr getCastOperator() const override {
    return nullptr;
  }
};

// Example connector factory (stubbed).
class AcmeConnectorFactory final : public connector::ConnectorFactory {
 public:
  AcmeConnectorFactory() : ConnectorFactory("acme") {}

  std::shared_ptr<connector::Connector> newConnector(
      const std::string& id,
      std::shared_ptr<const config::ConfigBase> cfg,
      folly::Executor* ex = nullptr) override {
    BOLT_NYI("acme connector implementation");
  }
};

} // namespace

extern "C" bool boltPluginInitV1(plugin::PluginRegistrar& registrar) {
  registrar.setMetadata(plugin::PluginMetadata{
      .name = "com.acme.analytics",
      .version = "1.0.0",
      .boltAbi = "bolt-plugin-v1",
      .vendor = "acme"});

  registrar.addScalarFunction(plugin::ScalarFunctionSpec{
      .name = "acme_add_one",
      .signatures = acmeAddOneSignatures(),
      .factory = acmeAddOneFactory(),
      .metadata = {},
      .overwrite = true});

  registrar.addLogicalType(plugin::LogicalTypeSpec{
      .name = "ACME_TAG",
      .factories = std::make_unique<TinyTagFactories>()});

  registrar.addConnectorFactory(plugin::ConnectorFactorySpec{
      .connectorName = "acme",
      .factory = std::make_shared<AcmeConnectorFactory>()});

  return true;
}

extern "C" void boltPluginShutdownV1() {
  // Optional plugin-level cleanup.
}
```

What the host does at load time:

1. `PluginManager` loads the shared library and resolves `boltPluginInitV1`.
2. Host creates an internal `PluginRegistrar` implementation.
3. Host calls `boltPluginInitV1(registrar)`.
4. Host validates each contributed spec and installs it in the correct registry
   (e.g. scalar specs go through `registerStatefulVectorFunction`, aggregate
   specs go through `registerAggregateFunction`).
5. Host tracks ownership of every installed entry under this plugin id.

### Downstream Usage (Current API)

`QueryCtx` exposes plugin loading in two ways:

1. dynamic path loading (`loadPlugin`)
2. in-process plugin object loading (`addPlugin`)

#### A) Dynamic `.so` load before planning/execution

```cpp
auto queryCtx = core::QueryCtx::create();
queryCtx->loadPlugin("/opt/bolt/plugins/libacme_plugin.so");

// Proceed with normal planning/execution using queryCtx.
```

#### B) Auto-load plugins from `QueryConfig`

`QueryConfig::kPluginLoadPaths` (`plugin.load_paths`) accepts a
comma-separated list:

```cpp
std::unordered_map<std::string, std::string> cfg{
    {core::QueryConfig::kPluginLoadPaths,
     "/opt/bolt/plugins/libacme_a.so,/opt/bolt/plugins/libacme_b.so"}};

auto queryCtx = core::QueryCtx::create(nullptr, core::QueryConfig{std::move(cfg)});
```

`QueryCtx` constructor parses this value and loads each path through
`PluginManager`.

#### C) In-process plugin object registration

```cpp
class AcmePlugin final : public plugin::IPlugin {
 public:
  const std::string& name() const override { return name_; }

  void registerInto(plugin::PluginRegistrar& registrar) override {
    registrar.addScalarFunction(plugin::ScalarFunctionSpec{
        .name = "acme_plus_one",
        .signatures = acmeSignatures(),
        .factory = acmeFactory(),
        .metadata = {},
        .overwrite = true});
  }

 private:
  const std::string name_{"acme.inproc"};
};

auto queryCtx = core::QueryCtx::create();
queryCtx->addPlugin(std::make_shared<AcmePlugin>());
```

Semantics:

- `loadPlugin(path)` returns `true` only when newly loaded in this manager.
- `addPlugin(plugin)` returns `true` only when newly added by plugin name.
- duplicate path/name loads are idempotent (`false`).

## Runtime Components

### PluginManager

Responsibilities:

- Load plugin shared library (`dlopen`/`dlsym`).
- Verify plugin ABI string/version.
- Execute init/shutdown entrypoints.
- Track ownership of registrations by plugin id.
- Support explicit unload and host shutdown cleanup.

### Domain Registries (Adapters)

Adapters map typed plugin specs to existing registries:

- Function registry adapter:
  - wraps existing function registration utilities.
- Operator adapter:
  - wraps `exec::Operator::registerOperator` and manages plugin-owned translators.
- Optimizer adapter (new):
  - introduces planner pass pipeline + per-stage rule registry.
- Logical type adapter:
  - wraps `registerCustomType` and cast operator hooks.
- Vector format adapter:
  - wraps vector serde/data layout registration.
- Connector adapter:
  - wraps `registerConnectorFactory` and optionally connector instances.

## Proposed File Layout

### Public plugin API (stable)

- `bolt/plugin/api/PluginAPI.h`
- `bolt/plugin/api/PluginRegistrar.h`
- `bolt/plugin/api/PluginMetadata.h`

### Runtime loader and host glue (internal)

- `bolt/plugin/runtime/PluginManager.h`
- `bolt/plugin/runtime/PluginManager.cpp`
- `bolt/plugin/runtime/DynamicLibrary.h`
- `bolt/plugin/runtime/DynamicLibrary.cpp`
- `bolt/plugin/runtime/PluginRegistry.h`
- `bolt/plugin/runtime/PluginRegistry.cpp`

### Domain adapters (internal)

- `bolt/plugin/adapters/FunctionPluginAdapter.{h,cpp}`
- `bolt/plugin/adapters/OperatorPluginAdapter.{h,cpp}`
- `bolt/plugin/adapters/OptimizerPluginAdapter.{h,cpp}`
- `bolt/plugin/adapters/LogicalTypePluginAdapter.{h,cpp}`
- `bolt/plugin/adapters/VectorFormatPluginAdapter.{h,cpp}`
- `bolt/plugin/adapters/ConnectorPluginAdapter.{h,cpp}`

### Built-in plugins (first-party)

- `bolt/plugins/builtin/functions/prestosql/Plugin.cpp`
- `bolt/plugins/builtin/functions/sparksql/Plugin.cpp`
- `bolt/plugins/builtin/connectors/hive/Plugin.cpp`
- `bolt/plugins/builtin/connectors/tpch/Plugin.cpp`
- `bolt/plugins/builtin/types/prestosql/Plugin.cpp`
- `bolt/plugins/builtin/vector/presto/Plugin.cpp`
- `bolt/plugins/builtin/operators/core/Plugin.cpp`
- `bolt/plugins/builtin/optimizers/core/Plugin.cpp`

### Integration points

- `bolt/core/Context` or startup path:
  - initialize `PluginManager`
  - load configured plugin list
  - load built-in plugin set by default
- user-facing runtime API (required for downstream usability):
  - `PluginManager::addPlugin(std::shared_ptr<plugin::IPlugin> plugin)` for in-process/static plugins.
  - `PluginManager::loadPlugin(const std::string& path)` for shared-library plugins.
  - `PluginManager::loadPlugins(const std::vector<std::string>& paths)`.
  - `PluginManager::isLoaded(const std::string& pluginName) const`.
  - `PluginManager::loadedPlugins() const`.
- query/planning API integration:
  - expose plugin manager through query/session context used by planning.
  - canonical query-scoped API: `core::QueryCtx::loadPlugin(const std::string& path)`.
  - allow plugin load before `PlanBuilder` construction and during planning phase.
  - enforce boundary: once a plan is finalized for execution, plugin set for that plan is immutable.

Example API usage (target):

```cpp
auto pluginManager = std::make_shared<plugin::PluginManager>();
pluginManager->loadPlugin("/opt/acme/libacme_bolt_plugin.so");
// or: pluginManager->addPlugin(std::make_shared<AcmeBuiltinPlugin>());

exec::test::PlanBuilder builder(pool.get());
builder.setPluginManager(pluginManager); // plugins visible to planning
auto plan = builder.project({"acme_add_one(c0)"}).planNode();
```

- Config keys:
  - `plugin.dir`
  - `plugin.load` (comma-separated ids or paths)
  - `plugin.allow_unload` (default false in prod)

## How API Is Exposed

- Install and export only `bolt/plugin/api/*` as public extension headers.
- Keep runtime/adapters private to the engine.
- External plugin developers include only public API and existing public engine headers needed to define UDFs/operators/types.

## Migration Plan (Incremental)

1. Add `PluginManager` + API headers + typed specs (no behavior changes).
2. Implement registrar-to-registry adapters for functions/operators/connectors/types/vector formats.
3. Wrap existing built-ins as in-process built-in plugins (still statically linked).
4. Move startup registration to `PluginManager`-driven registration.
5. Add user-facing APIs: `addPlugin`, `loadPlugin`, `loadPlugins`, `loadedPlugins`.
6. Expose plugin manager in planning path (`Context` and `PlanBuilder` integration).
7. Enforce planning/execution boundary for plugin mutability (freeze plugin set per plan).
8. Add dynamic loading of external plugins from configured paths.
9. Add optimizer plugin pipeline (new registry).
10. Add unload safety guardrails and plugin ownership tracking.
11. Add end-to-end tests:
  - plugin loaded before planning can contribute function/operator/type/connector.
  - plugin loaded during planning is visible to subsequent planning operations.
  - plugin loaded after plan finalization does not mutate already-built plan behavior.

## Open Design Decisions

- ABI stability strategy:
  - same-toolchain C++ ABI (faster rollout) vs strict C ABI handles.
- Unload policy:
  - production-safe default is load-once/no-unload.
- Optimizer API granularity:
  - whole-plan pass vs rule-level registration with stage ordering.
- Vector format API:
  - serde-only first vs full physical encoding registration in v1.

## Unregistration and Lifecycle

V1 lifecycle policy:

- Default production policy: load-once, process-lifetime plugins.
- Optional unload only where underlying registries support safe per-entry removal and there is no in-flight usage.

Future path (for automatic rollback):

- add ownership-aware registries (entry -> plugin id).
- add per-entry remove APIs everywhere.
- add quiescence checks so unload happens only when no task references plugin-owned artifacts.

## Next Steps

Implement in this order:

- land `bolt/plugin/api/*`
- land `PluginManager`
- create built-in adapter plugin for one vertical slice (e.g. Presto scalar+aggregate functions)
- switch one startup path to PluginManager as proof of integration
- add `addPlugin` and `loadPlugin` user APIs
- migrate remaining built-ins and enable downstream plugin usage in normal query flow
