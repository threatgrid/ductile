# OpenSearch Support - Implementation Summary

This document provides a technical overview of the OpenSearch support implementation in Ductile.

## Architecture Overview

The implementation adds OpenSearch 2.x/3.x support through a layered architecture that maintains backward compatibility with existing Elasticsearch code.

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Code                         │
│         (unchanged - uses existing Ductile API)             │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                    Public API Layer                         │
│   ductile.document  ductile.index  ductile.lifecycle        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                 Feature Detection Layer                     │
│              ductile.features  ductile.capabilities         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│                   Connection Layer                          │
│                      ductile.conn                           │
│              (engine-aware HTTP requests)                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│              Elasticsearch 7.x  │  OpenSearch 2.x/3.x       │
└─────────────────────────────────────────────────────────────┘
```

## New Modules

### ductile.capabilities

**Purpose:** Engine detection and version parsing.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `parse-version` | Parses version string (e.g., "2.19.0") into `{:major :minor :patch}` |
| `get-cluster-info` | Fetches cluster info from root endpoint |
| `detect-engine` | Detects engine type from cluster info response |
| `verify-connection` | Verifies connection and returns engine info |
| `version-compare` | Compares two version maps |
| `version>=?` / `version<?` | Version comparison predicates |

**Engine Detection Logic:**

```clojure
;; OpenSearch includes a "distribution" field in version info
{:version {:distribution "opensearch" :number "2.19.0" ...}}

;; Elasticsearch does not have this field
{:version {:number "7.17.0" :build_flavor "default" ...}}
```

### ductile.features

**Purpose:** Feature compatibility detection based on engine and version.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `supports-ilm?` | ILM support (ES 7+ only) |
| `supports-ism?` | ISM support (OpenSearch only) |
| `supports-data-streams?` | Data streams (ES 7+, OS 2+) |
| `supports-composable-templates?` | Composable templates (ES 7+, OS 1+) |
| `supports-legacy-templates?` | Legacy templates (all versions, deprecated) |
| `supports-doc-types?` | Document types (ES < 7 only) |
| `lifecycle-management-type` | Returns `:ilm`, `:ism`, or `nil` |
| `get-feature-summary` | Returns map of all feature support |
| `require-feature!` | Throws if feature not supported |

**Feature Matrix:**

| Feature | ES 7.x | OS 2.x | OS 3.x |
|---------|--------|--------|--------|
| ILM | Yes | No | No |
| ISM | No | Yes | Yes |
| Data Streams | Yes | Yes | Yes |
| Composable Templates | Yes | Yes | Yes |
| Legacy Templates | Yes (deprecated) | Yes (deprecated) | Yes (deprecated) |
| Document Types | No | No | No |

### ductile.lifecycle

**Purpose:** Lifecycle policy management with automatic ILM/ISM transformation.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `transform-ilm-to-ism` | Transforms ILM policy to ISM format |
| `transform-ism-to-ilm` | Transforms ISM policy to ILM format |
| `normalize-policy` | Normalizes policy for target engine |
| `policy-uri` | Builds correct URI based on engine |
| `create-policy!` | Creates policy (auto-transforms for OpenSearch) |
| `get-policy` | Gets policy with response normalization |
| `delete-policy!` | Deletes policy |

**ILM to ISM Transformation:**

The transformation maps ILM concepts to ISM equivalents:

| ILM | ISM | Notes |
|-----|-----|-------|
| Phases | States | hot, warm, cold, delete |
| `min_age` on phase | Transition condition | `min_index_age` |
| `rollover.max_docs` | `rollover.min_doc_count` | |
| `rollover.max_age` | `rollover.min_index_age` | |
| `rollover.max_size` | `rollover.min_size` | |
| `readonly` action | `read_only` action | |
| `force_merge` action | `force_merge` action | Same structure |
| `shrink` action | `shrink` action | `number_of_shards` → `num_new_shards` |
| `delete` action | `delete` action | Same structure |

**Unsupported ILM Actions:**

These ILM actions are silently ignored (logged as warning):
- `set_priority` - No ISM equivalent
- `allocate` - No direct ISM equivalent
- `migrate` - No ISM equivalent

## Modified Modules

### ductile.conn

**Changes:**
- Added `:engine` parameter to `connect` function
- Default engine is `:elasticsearch` for backward compatibility
- Engine is stored in connection map and passed to all operations

```clojure
;; Connection schema now includes :engine
{:uri "http://localhost:9200"
 :version 7
 :engine :elasticsearch  ; or :opensearch
 :cm <connection-manager>
 :request-fn <fn>
 :auth {...}}
```

### ductile.schemas

**Changes:**
- Added `:engine` to `ConnectParams` schema (optional, defaults to `:elasticsearch`)
- Added `:engine` to `ESConn` schema (required)

```clojure
(s/defschema ConnectParams
  {:host s/Str
   :port s/Int
   (s/optional-key :engine) (s/enum :elasticsearch :opensearch)
   ...})

(s/defschema ESConn
  {:uri s/Str
   :version s/Int
   :engine (s/enum :elasticsearch :opensearch)
   ...})
```

### ductile.index

**Changes:**
- Replaced version checks with feature checks
- Moved policy operations to `ductile.lifecycle` namespace
- Data stream and template operations now use feature detection

```clojure
;; Before
(when (< version 7)
  (throw (ex-info "Cannot create datastream for Elasticsearch version < 7" conn)))

;; After
(when-not (feat/supports-data-streams? conn)
  (throw (ex-info "Data streams not supported on this engine/version" conn)))
```

## Test Infrastructure

### Multi-Engine Test Helper

The `for-each-es-version` macro runs tests against all configured engines:

```clojure
(defmacro for-each-es-version [msg clean & body]
  ;; Runs body for each engine/version pair
  ;; Provides anaphoric bindings: engine, version, conn
  ...)
```

**Engine Configuration:**

Set `DUCTILE_TEST_ENGINES` environment variable:
- `es` - Elasticsearch only
- `os` - OpenSearch only
- `all` - Both (default)

### Docker Test Containers

| Engine | Version | Port | Auth |
|--------|---------|------|------|
| Elasticsearch | 7.10.1 | 9207 | elastic/ductile |
| OpenSearch | 2.19.0 | 9202 | (security disabled) |
| OpenSearch | 3.1.0 | 9203 | (security disabled) |

## API Endpoints

### Policy Endpoints

| Engine | Create/Update | Get | Delete |
|--------|---------------|-----|--------|
| Elasticsearch | `PUT _ilm/policy/{name}` | `GET _ilm/policy/{name}` | `DELETE _ilm/policy/{name}` |
| OpenSearch | `PUT _plugins/_ism/policies/{name}` | `GET _plugins/_ism/policies/{name}` | `DELETE _plugins/_ism/policies/{name}` |

### Response Normalization

OpenSearch responses are normalized to match Elasticsearch format:

```clojure
;; OpenSearch create response
{:_id "policy-name" :_version 1 ...}

;; Normalized to match ES
{:acknowledged true}

;; OpenSearch get response
{:_id "policy-name" :policy {:states [...] ...}}

;; Normalized to match ES structure
{:policy-name {:policy {:states [...] ...}}}
```

## Backward Compatibility

The implementation maintains 100% backward compatibility:

1. **Default engine:** Without `:engine` parameter, defaults to `:elasticsearch`
2. **Existing tests:** All existing tests pass without modification
3. **API surface:** No breaking changes to public functions
4. **Response format:** OpenSearch responses normalized to match ES format

## Performance Considerations

1. **No runtime overhead** for Elasticsearch connections
2. **Policy transformation** happens once at creation time
3. **Feature checks** are simple comparisons (no network calls)
4. **Connection pooling** works identically for both engines

## Error Handling

Unsupported operations throw `ex-info` with structured data:

```clojure
(throw (ex-info "Data streams not supported on this engine/version"
                {:type ::unsupported-feature
                 :feature :data-streams
                 :engine :opensearch
                 :version 1}))
```

## Future Considerations

1. **OpenSearch 1.x support** - Could be added if needed
2. **Elasticsearch 8.x support** - May require additional changes
3. **ISM-specific features** - Some ISM features have no ILM equivalent
4. **Security integration** - OpenSearch security plugin differs from X-Pack
