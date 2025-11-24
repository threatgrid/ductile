# Ductile

[![Clojars Project](https://img.shields.io/clojars/v/threatgrid/ductile.svg)](https://clojars.org/threatgrid/ductile)

A minimalist Clojure library for Elasticsearch and OpenSearch REST APIs.

## Features

- **Multi-Engine Support**: Works transparently with both Elasticsearch 7.x and OpenSearch 2.x/3.x
- **Pure REST API**: No heavyweight Java client dependencies
- **Automatic Transformation**: ILM policies automatically transform to ISM for OpenSearch
- **Feature Detection**: Automatically detects and adapts to engine capabilities
- **Backward Compatible**: Existing Elasticsearch code works without changes

## Compatibility

| Engine | Versions | Status |
|--------|----------|--------|
| Elasticsearch | 7.x | ✅ Full Support |
| OpenSearch | 2.x, 3.x | ✅ Full Support |
| Elasticsearch | 5.x, 6.x | ⚠️ Deprecated (until 0.4.9) |

## Changes

- **0.6.0** (Current)
  - **NEW**: Full OpenSearch 2.x and 3.x support
  - **NEW**: Automatic ILM to ISM policy transformation
  - **NEW**: Engine detection and feature compatibility layer
  - **NEW**: Multi-engine test infrastructure
- 0.5.0
  - Remove ES5 support, add aliases support
- 0.4.5
  - Fix: Ensure UTF-8 encoding for bulk insert operations
- 0.4.4
  - Fix: preserve field order when sorting by multiple fields

## Installation

```clojure
[threatgrid/ductile "0.6.0"]
```

## Usage

### Create a Connection

#### Elasticsearch

```clojure
(require '[ductile.conn :as es-conn])

;; Connect to Elasticsearch (default engine)
(def es-conn (es-conn/connect {:host "localhost"
                               :port 9200
                               :version 7
                               :protocol :http
                               :auth {:type :basic-auth
                                      :params {:user "elastic" :pwd "password"}}}))
```

#### OpenSearch

```clojure
;; Connect to OpenSearch - just specify :engine
(def os-conn (es-conn/connect {:host "localhost"
                               :port 9200
                               :engine :opensearch    ; ← Specify OpenSearch
                               :version 2
                               :protocol :http
                               :auth {:type :basic-auth
                                      :params {:user "admin" :pwd "password"}}}))
```

**Connection Parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `:host` | ✅ | - | Hostname or IP address |
| `:port` | ✅ | - | Port number |
| `:engine` | ❌ | `:elasticsearch` | Engine type (`:elasticsearch` or `:opensearch`) |
| `:version` | ❌ | `7` | Major version number |
| `:protocol` | ❌ | `:http` | Protocol (`:http` or `:https`) |
| `:timeout` | ❌ | `30000` | Request timeout in milliseconds |
| `:auth` | ❌ | none | Authentication configuration |

### Authentication

Ductile supports multiple authentication methods:

#### Basic Auth

```clojure
{:type :basic-auth
 :params {:user "username" :pwd "password"}}
```

#### API Key

```clojure
{:type :api-key
 :params {:id "key-id"
          :api-key "key-secret"}}
```

#### OAuth Token

```clojure
{:type :oauth-token
 :params {:token "your-token"}}
```

#### Bearer Token

```clojure
{:type :bearer
 :params {:token "your-token"}}
```

#### Custom Headers

```clojure
{:type :headers
 :params {:authorization "ApiKey base64-encoded-key"}}
```

### Engine Detection

Ductile can automatically detect the engine type and version:

```clojure
(require '[ductile.capabilities :as cap])

;; Auto-detect engine and version
(cap/verify-connection conn)
;; => {:engine :opensearch
;;     :version {:major 2 :minor 19 :patch 0}}
```

### Feature Detection

Check what features are available for your engine:

```clojure
(require '[ductile.features :as feat])

;; Check specific features
(feat/supports-ilm? conn)              ; => true for ES 7+, false for OpenSearch
(feat/supports-ism? conn)              ; => true for OpenSearch, false for ES
(feat/supports-data-streams? conn)     ; => true for ES 7+ and OpenSearch 2+
(feat/lifecycle-management-type conn)  ; => :ilm or :ism

;; Get complete feature summary
(feat/get-feature-summary conn)
;; => {:ilm false
;;     :ism true
;;     :data-streams true
;;     :composable-templates true
;;     :legacy-templates true
;;     :doc-types false}
```

### Index Operations

Index operations work identically on both Elasticsearch and OpenSearch:

```clojure
(require '[ductile.index :as es-index])

;; Check if index exists
(es-index/index-exists? conn "my-index")
;; => false

;; Create index with configuration
(def index-config
  {:settings {:number_of_shards 3
              :number_of_replicas 1
              :refresh_interval "1s"}
   :mappings {:properties {:name {:type :text}
                           :age {:type :long}
                           :created_at {:type :date}}}
   :aliases {:my-index-alias {}}})

(es-index/create! conn "my-index" index-config)

;; Manage index lifecycle
(es-index/close! conn "my-index")
(es-index/open! conn "my-index")
(es-index/delete! conn "my-index")

;; Refresh index
(es-index/refresh! conn "my-index")
```

### Index Templates

```clojure
;; Create composable index template (ES 7.8+, OpenSearch 1+)
(es-index/create-index-template! conn "my-template" index-config ["logs-*" "metrics-*"])

;; Get template
(es-index/get-index-template conn "my-template")

;; Delete template
(es-index/delete-index-template! conn "my-template")

;; Legacy templates also supported
(es-index/create-template! conn "legacy-template" index-config ["old-*"])
```

### Lifecycle Management (ILM/ISM)

**The same API works for both Elasticsearch ILM and OpenSearch ISM!**

```clojure
;; Define policy in ILM format (works for both engines)
(def rollover-policy
  {:phases
   {:hot {:min_age "0ms"
          :actions {:rollover {:max_docs 10000000
                               :max_age "7d"}}}
    :warm {:min_age "7d"
           :actions {:readonly {}
                     :force_merge {:max_num_segments 1}}}
    :delete {:min_age "30d"
             :actions {:delete {}}}}})

;; Create policy - automatically transforms to ISM for OpenSearch
(require '[ductile.lifecycle :as lifecycle])
(lifecycle/create-policy! conn "my-rollover-policy" rollover-policy)

;; Get policy (returns ILM format for ES, ISM format for OpenSearch)
(lifecycle/get-policy conn "my-rollover-policy")

;; Delete policy
(lifecycle/delete-policy! conn "my-rollover-policy")
```

**How it works:**

- For **Elasticsearch**: Uses ILM (Index Lifecycle Management) directly
- For **OpenSearch**: Automatically transforms ILM policy to ISM (Index State Management) format
- Your code doesn't change - the transformation happens transparently

**Example transformation:**

```clojure
;; Input (ILM format)
{:phases {:hot {:actions {:rollover {:max_docs 100000}}}
          :delete {:min_age "30d" :actions {:delete {}}}}}

;; Automatically becomes (ISM format for OpenSearch)
{:states [{:name "hot"
           :actions [{:rollover {:min_doc_count 100000}}]
           :transitions [{:state_name "delete"
                          :conditions {:min_index_age "30d"}}]}
          {:name "delete"
           :actions [{:delete {}}]}]
 :default_state "hot"
 :schema_version 1}
```

### Document Operations

CRUD operations work identically on both engines:

```clojure
(require '[ductile.document :as doc])

;; Create document
(doc/create-doc conn "my-index"
  {:id 1
   :name "John Doe"
   :email "john@example.com"}
  {:refresh "wait_for"})

;; Get document
(doc/get-doc conn "my-index" 1 {})
;; => {:id 1 :name "John Doe" :email "john@example.com"}

;; Update document
(doc/update-doc conn "my-index" 1
  {:age 30}
  {:refresh "wait_for"})

;; Delete document
(doc/delete-doc conn "my-index" 1 {:refresh "wait_for"})

;; Bulk operations
(doc/bulk-index-docs conn "my-index"
  [{:id 1 :name "Alice"}
   {:id 2 :name "Bob"}
   {:id 3 :name "Charlie"}]
  {:refresh "true"})

;; Delete by query
(doc/delete-by-query conn ["my-index"]
  {:match {:status "archived"}}
  {:wait_for_completion true :refresh "true"})
```

### Queries

```clojure
(require '[ductile.query :as q])

;; Simple query
(doc/query conn "my-index"
  {:match {:name "John"}}
  {})

;; Query with aggregations
(doc/query conn "my-index"
  {:match_all {}}
  {:aggs {:age_stats {:stats {:field :age}}}})

;; Using query helpers
(doc/query conn "my-index"
  (q/bool {:must [{:match {:status "active"}}]
           :filter [{:range {:age {:gte 18}}}]})
  {:limit 100})

;; Search with filters
(doc/search-docs conn "my-index"
  {:query_string {:query "active"}}
  {:age 30}
  {:sort {:created_at {:order :desc}}})
```

### Data Streams

Data streams work on both Elasticsearch 7.9+ and OpenSearch 2.0+:

```clojure
;; Create data stream
(es-index/create-data-stream! conn "logs-app")

;; Get data stream info
(es-index/get-data-stream conn "logs-app")

;; Delete data stream
(es-index/delete-data-stream! conn "logs-app")
```

## Feature Compatibility Matrix

| Feature | Elasticsearch 7 | OpenSearch 2 | OpenSearch 3 | Notes |
|---------|----------------|--------------|--------------|-------|
| Basic CRUD | ✅ | ✅ | ✅ | Full compatibility |
| Queries & Aggregations | ✅ | ✅ | ✅ | Full compatibility |
| Index Management | ✅ | ✅ | ✅ | Full compatibility |
| Index Templates | ✅ | ✅ | ✅ | Both legacy and composable |
| Data Streams | ✅ (7.9+) | ✅ | ✅ | Requires version check |
| ILM Policies | ✅ | ⚠️ Auto-transform | ⚠️ Auto-transform | Transforms to ISM |
| ISM Policies | ❌ | ✅ | ✅ | OpenSearch only |
| Rollover | ✅ | ✅ | ✅ | Full compatibility |
| Aliases | ✅ | ✅ | ✅ | Full compatibility |

⚠️ = Automatically handled via transformation layer

## Migration from Elasticsearch to OpenSearch

### Zero-Code Migration

If your application only uses basic operations (CRUD, queries, indices), migration is as simple as:

```clojure
;; Before (Elasticsearch)
(def conn (es-conn/connect {:host "es-host" :port 9200 :version 7}))

;; After (OpenSearch) - just add :engine
(def conn (es-conn/connect {:host "os-host"
                            :port 9200
                            :engine :opensearch  ; ← Only change needed
                            :version 2}))
```

### ILM to ISM Migration

If you use ILM policies, no code changes are required! Policies are automatically transformed:

```clojure
;; This code works for BOTH Elasticsearch and OpenSearch
(require '[ductile.lifecycle :as lifecycle])

(defn setup-lifecycle [conn]
  (lifecycle/create-policy! conn "my-policy"
    {:phases {:hot {:actions {:rollover {:max_docs 1000000}}}
              :delete {:min_age "30d" :actions {:delete {}}}}}))

;; Works with Elasticsearch (creates ILM policy)
(setup-lifecycle es-conn)

;; Works with OpenSearch (creates ISM policy with auto-transformation)
(setup-lifecycle os-conn)
```

### Configuration-Based Migration

Use environment variables or configuration to switch engines:

```clojure
(defn create-connection [config]
  (es-conn/connect
    {:host (:host config)
     :port (:port config)
     :engine (keyword (:engine config))  ; "elasticsearch" or "opensearch"
     :version (:version config)
     :auth {:type :basic-auth
            :params {:user (:user config)
                     :pwd (:password config)}}}))

;; Configuration switches engine
(def config {:host "localhost"
             :port 9200
             :engine "opensearch"  ; ← Switch here
             :version 2
             :user "admin"
             :password "password"})

(def conn (create-connection config))
```

## Testing

### Running Tests

```bash
# Run unit tests only
lein test ductile.capabilities-test ductile.conn-test ductile.features-test ductile.lifecycle-test

# Run with Docker containers
cd containers
docker-compose up -d

# Test against all engines
DUCTILE_TEST_ENGINES=all lein test :integration

# Test against Elasticsearch only
DUCTILE_TEST_ENGINES=es lein test :integration

# Test against OpenSearch only
DUCTILE_TEST_ENGINES=os lein test :integration
```

### Test Stubbing

```clojure
(require '[ductile.conn :as es-conn]
         '[clj-http.client :as client])

;; Stub requests for testing
(def conn (es-conn/connect
            {:host "localhost"
             :port 9200
             :request-fn (fn [req]
                          {:status 200
                           :body {:acknowledged true}})}))
```

## Advanced Usage

### Custom Request Function

```clojure
(def conn (es-conn/connect
            {:host "localhost"
             :port 9200
             :request-fn (-> (fn [req]
                              (println "Request:" req)
                              (client/request req))
                            client/wrap-query-params)}))
```

### Connection Pooling

Ductile automatically manages connection pooling with sensible defaults:

- 100 threads
- 100 connections per route
- Configurable timeout

### Error Handling

```clojure
(try
  (doc/create-doc conn "my-index" {:id 1 :name "test"} {})
  (catch clojure.lang.ExceptionInfo e
    (let [data (ex-data e)]
      (case (:type data)
        :ductile.conn/unauthorized (println "Auth failed")
        :ductile.conn/invalid-request (println "Invalid request")
        :ductile.conn/es-unknown-error (println "Unknown error")
        (throw e)))))
```

## Docker Support

Test containers are provided for local development:

```bash
cd containers
docker-compose up -d

# Services:
# - es7: Elasticsearch 7.10.1 on port 9207
# - opensearch2: OpenSearch 2.19.0 on port 9202
# - opensearch3: OpenSearch 3.1.0 on port 9203
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Run tests: `lein test`
4. Submit a pull request

## License

Copyright © Cisco Systems

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

## Support

For issues and feature requests, please use the GitHub issue tracker.
