# OpenSearch Migration Guide

This guide covers migrating from Elasticsearch to OpenSearch using Ductile.

## Overview

Ductile 0.6.0 introduces transparent OpenSearch support. Applications can migrate from Elasticsearch to OpenSearch by changing a single connection parameter - no code changes required for most use cases.

## Quick Start

### Before (Elasticsearch)

```clojure
(require '[ductile.conn :as es-conn])

(def conn (es-conn/connect {:host "es-host"
                            :port 9200
                            :version 7
                            :auth {:type :basic-auth
                                   :params {:user "elastic" :pwd "password"}}}))
```

### After (OpenSearch)

```clojure
(require '[ductile.conn :as es-conn])

(def conn (es-conn/connect {:host "os-host"
                            :port 9200
                            :engine :opensearch    ; <-- Only change needed
                            :version 2
                            :auth {:type :basic-auth
                                   :params {:user "admin" :pwd "password"}}}))
```

## Migration Scenarios

### Scenario 1: Basic CRUD Operations

**No changes required.** All document operations work identically:

```clojure
(require '[ductile.document :as doc])

;; These work on both Elasticsearch and OpenSearch
(doc/create-doc conn "my-index" {:id 1 :name "test"} {:refresh "wait_for"})
(doc/get-doc conn "my-index" 1 {})
(doc/update-doc conn "my-index" 1 {:status "updated"} {:refresh "wait_for"})
(doc/delete-doc conn "my-index" 1 {:refresh "wait_for"})
```

### Scenario 2: Index Management

**No changes required.** Index operations work identically:

```clojure
(require '[ductile.index :as es-index])

;; These work on both engines
(es-index/create! conn "my-index" {:settings {:number_of_shards 1}})
(es-index/index-exists? conn "my-index")
(es-index/delete! conn "my-index")
```

### Scenario 3: Index Templates

**No changes required** for composable templates (recommended):

```clojure
;; Composable templates work on both engines
(es-index/create-index-template! conn "my-template"
  {:settings {:number_of_shards 1}
   :mappings {:properties {:name {:type :text}}}}
  ["my-index-*"])
```

**Note:** Legacy templates (`_template` API) are deprecated. Use composable templates (`_index_template` API) for new code.

### Scenario 4: Lifecycle Policies (ILM/ISM)

**No code changes required.** Ductile automatically transforms ILM policies to ISM format:

```clojure
(require '[ductile.lifecycle :as lifecycle])

;; Define policy in ILM format - works for BOTH engines
(def rollover-policy
  {:phases
   {:hot {:actions {:rollover {:max_docs 10000000
                               :max_age "7d"}}}
    :warm {:min_age "7d"
           :actions {:readonly {}
                     :force_merge {:max_num_segments 1}}}
    :delete {:min_age "30d"
             :actions {:delete {}}}}})

;; Create policy - automatically uses ILM or ISM based on engine
(lifecycle/create-policy! conn "my-policy" rollover-policy)
```

#### How Transformation Works

When connecting to OpenSearch, ILM policies are automatically transformed:

| ILM Concept | ISM Equivalent |
|-------------|----------------|
| Phase | State |
| `max_docs` | `min_doc_count` |
| `max_age` | `min_index_age` |
| `max_size` | `min_size` |
| Phase transitions | State transitions with conditions |

**Example Transformation:**

```clojure
;; Input (ILM format)
{:phases {:hot {:actions {:rollover {:max_docs 100000}}}
          :delete {:min_age "30d" :actions {:delete {}}}}}

;; Output (ISM format for OpenSearch)
{:states [{:name "hot"
           :actions [{:rollover {:min_doc_count 100000}}]
           :transitions [{:state_name "delete"
                          :conditions {:min_index_age "30d"}}]}
          {:name "delete"
           :actions [{:delete {}}]}]
 :default_state "hot"
 :schema_version 1}
```

### Scenario 5: Data Streams

**No changes required.** Data streams work on both ES 7.9+ and OpenSearch 2.0+:

```clojure
(es-index/create-data-stream! conn "logs-app")
(es-index/get-data-stream conn "logs-app")
(es-index/delete-data-stream! conn "logs-app")
```

## Feature Detection

Use feature detection to write code that adapts to the engine:

```clojure
(require '[ductile.features :as feat])

;; Check specific features
(feat/supports-ilm? conn)              ; true for ES 7+
(feat/supports-ism? conn)              ; true for OpenSearch
(feat/supports-data-streams? conn)     ; true for ES 7+ and OS 2+
(feat/lifecycle-management-type conn)  ; :ilm or :ism

;; Get complete feature summary
(feat/get-feature-summary conn)
;; => {:ilm false
;;     :ism true
;;     :data-streams true
;;     :composable-templates true
;;     :legacy-templates true
;;     :doc-types false}
```

## Configuration-Based Migration

Use environment variables or configuration to switch engines without code changes:

```clojure
(defn create-connection [config]
  (es-conn/connect
    {:host (:search-host config)
     :port (:search-port config)
     :engine (keyword (:search-engine config))  ; "elasticsearch" or "opensearch"
     :version (:search-version config)
     :auth {:type :basic-auth
            :params {:user (:search-user config)
                     :pwd (:search-password config)}}}))

;; Environment-based configuration
(def config
  {:search-host (or (System/getenv "SEARCH_HOST") "localhost")
   :search-port (Integer/parseInt (or (System/getenv "SEARCH_PORT") "9200"))
   :search-engine (or (System/getenv "SEARCH_ENGINE") "elasticsearch")
   :search-version (Integer/parseInt (or (System/getenv "SEARCH_VERSION") "7"))
   :search-user (System/getenv "SEARCH_USER")
   :search-password (System/getenv "SEARCH_PASSWORD")})

(def conn (create-connection config))
```

## Verifying Engine Detection

After connecting, verify the engine was detected correctly:

```clojure
(require '[ductile.capabilities :as cap])

(cap/verify-connection conn)
;; => {:engine :opensearch
;;     :version {:major 2 :minor 19 :patch 0}}
```

## Migration Checklist

- [ ] Update Ductile dependency to 0.6.0+
- [ ] Add `:engine :opensearch` to connection parameters
- [ ] Update `:version` to match your OpenSearch version (2 or 3)
- [ ] Update authentication credentials for OpenSearch
- [ ] Test CRUD operations
- [ ] Test index management operations
- [ ] Test lifecycle policies (if used)
- [ ] Test data streams (if used)
- [ ] Update CI/CD configuration to test against OpenSearch

## Troubleshooting

### Connection Fails

Ensure:
1. OpenSearch is running and accessible
2. Security plugin is configured correctly (or disabled for testing)
3. Credentials are correct

### Policy Creation Fails

Check:
1. The policy format is valid ILM format
2. OpenSearch ISM plugin is enabled
3. User has permissions to create ISM policies

### Feature Not Supported

Use `feat/get-feature-summary` to check what features are available:

```clojure
(feat/get-feature-summary conn)
```

## Supported Versions

| Engine | Version | Support |
|--------|---------|---------|
| Elasticsearch | 7.x | Full |
| OpenSearch | 2.x | Full |
| OpenSearch | 3.x | Full |
| Elasticsearch | 5.x, 6.x | Deprecated (until 0.4.9) |

## Getting Help

For issues and feature requests, use the [GitHub issue tracker](https://github.com/threatgrid/ductile/issues).
