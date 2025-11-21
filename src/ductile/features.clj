(ns ductile.features
  "Feature compatibility detection for Elasticsearch and OpenSearch"
  (:require [ductile.capabilities :as cap]
            [ductile.schemas :refer [ESConn]]
            [schema.core :as s]))

(s/defn supports-ilm? :- s/Bool
  "Check if engine supports ILM (Index Lifecycle Management).
   ILM is Elasticsearch 7.0+ only feature.
   OpenSearch uses ISM (Index State Management) instead.

   Usage:
     (supports-ilm? {:engine :elasticsearch :version 7})
     ;; => true

     (supports-ilm? {:engine :opensearch :version 2})
     ;; => false"
  [{:keys [engine version]} :- ESConn]
  (and (= engine :elasticsearch)
       (>= version 7)))

(s/defn supports-ism? :- s/Bool
  "Check if engine supports ISM (Index State Management).
   ISM is OpenSearch's equivalent to Elasticsearch's ILM.
   Available in all OpenSearch versions.

   Usage:
     (supports-ism? {:engine :opensearch :version {:major 2}})
     ;; => true

     (supports-ism? {:engine :elasticsearch :version {:major 7}})
     ;; => false"
  [{:keys [engine]} :- ESConn]
  (= engine :opensearch))

(s/defn supports-data-streams? :- s/Bool
  "Check if engine supports data streams.
   - Elasticsearch: 7.9+ (we conservatively assume all 7.x)
   - OpenSearch: 2.0+

   Note: ESConn only stores major version, so we assume all ES 7.x supports data streams.

   Usage:
     (supports-data-streams? {:engine :elasticsearch :version 7})
     ;; => true"
  [{:keys [engine version]} :- ESConn]
  (or (and (= engine :elasticsearch)
           (>= version 7))
      (and (= engine :opensearch)
           (>= version 2))))

(s/defn supports-composable-templates? :- s/Bool
  "Check if engine supports composable index templates.
   - Elasticsearch: 7.8+ (we conservatively assume all 7.x)
   - OpenSearch: 1.0+

   Note: ESConn only stores major version, so we assume all ES 7.x supports composable templates.

   Usage:
     (supports-composable-templates? {:engine :elasticsearch :version 7})
     ;; => true"
  [{:keys [engine version]} :- ESConn]
  (or (and (= engine :elasticsearch)
           (>= version 7))
      (and (= engine :opensearch)
           (>= version 1))))

(s/defn supports-legacy-templates? :- s/Bool
  "Check if engine supports legacy index templates.
   Supported in all versions but deprecated in:
   - Elasticsearch: 7.8+
   - OpenSearch: 1.0+

   Usage:
     (supports-legacy-templates? {:engine :elasticsearch :version 7})
     ;; => true"
  [_conn :- ESConn]
  ;; All versions support legacy templates
  true)

(s/defn supports-doc-types? :- s/Bool
  "Check if engine requires document types in URLs.
   Document types were removed in:
   - Elasticsearch: 7.0+
   - OpenSearch: All versions (based on ES 7.x)

   Usage:
     (supports-doc-types? {:engine :elasticsearch :version 5})
     ;; => true

     (supports-doc-types? {:engine :elasticsearch :version 7})
     ;; => false"
  [{:keys [engine version]} :- ESConn]
  (and (= engine :elasticsearch)
       (< version 7)))

(s/defn lifecycle-management-type :- (s/maybe (s/enum :ilm :ism))
  "Returns the lifecycle management type supported by the engine.
   - :ilm for Elasticsearch 7.0+
   - :ism for OpenSearch
   - nil for Elasticsearch < 7.0

   Usage:
     (lifecycle-management-type {:engine :elasticsearch :version 7})
     ;; => :ilm

     (lifecycle-management-type {:engine :opensearch :version 2})
     ;; => :ism"
  [conn :- ESConn]
  (cond
    (supports-ilm? conn) :ilm
    (supports-ism? conn) :ism
    :else nil))

(s/defn get-feature-summary :- {s/Keyword s/Bool}
  "Get a summary of all feature support for the given connection.

   Usage:
     (get-feature-summary {:engine :opensearch :version 2})
     ;; => {:ilm false
     ;;     :ism true
     ;;     :data-streams true
     ;;     :composable-templates true
     ;;     :legacy-templates true
     ;;     :doc-types false}"
  [conn :- ESConn]
  {:ilm (supports-ilm? conn)
   :ism (supports-ism? conn)
   :data-streams (supports-data-streams? conn)
   :composable-templates (supports-composable-templates? conn)
   :legacy-templates (supports-legacy-templates? conn)
   :doc-types (supports-doc-types? conn)})

(s/defn require-feature!
  "Throw an exception if the feature is not supported.

   Usage:
     (require-feature! conn :data-streams \"Data streams are required\")
     ;; Throws ex-info if data streams not supported"
  [conn :- ESConn
   feature :- s/Keyword
   message :- s/Str]
  (let [feature-checks {:ilm supports-ilm?
                        :ism supports-ism?
                        :data-streams supports-data-streams?
                        :composable-templates supports-composable-templates?
                        :doc-types supports-doc-types?}
        check-fn (get feature-checks feature)]
    (when-not (and check-fn (check-fn conn))
      (throw (ex-info message
                      {:type ::unsupported-feature
                       :feature feature
                       :engine (:engine conn)
                       :version (:version conn)})))))
