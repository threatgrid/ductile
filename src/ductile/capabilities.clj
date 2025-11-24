(ns ductile.capabilities
  "Engine detection and capability discovery for Elasticsearch and OpenSearch"
  (:require [clojure.string :as str]
            [ductile.conn :refer [make-http-opts safe-es-read]]
            [ductile.schemas :refer [ESConn]]
            [schema.core :as s]))

(s/defschema VersionInfo
  "Parsed version information"
  {:major s/Int
   :minor s/Int
   (s/optional-key :patch) s/Int})

(s/defschema EngineInfo
  "Detected engine information"
  {:engine (s/enum :elasticsearch :opensearch)
   :version VersionInfo})

(defn parse-version
  "Parse version string like '2.19.0' or '7.17.0' into components
   Returns {:major X :minor Y :patch Z}"
  [version-str]
  (when version-str
    (let [parts (str/split version-str #"\.")
          [major minor patch] (map #(Integer/parseInt %) parts)]
      (cond-> {:major major
               :minor minor}
        patch (assoc :patch patch)))))

(s/defn get-cluster-info
  "Fetch cluster info from root endpoint"
  [{:keys [uri request-fn] :as conn} :- ESConn]
  (-> (make-http-opts conn)
      (assoc :method :get
             :url uri)
      request-fn
      safe-es-read))

(s/defn detect-engine :- EngineInfo
  "Detect engine type and version from cluster info response.

   Elasticsearch response example:
   {:name \"node-1\"
    :cluster_name \"elasticsearch\"
    :version {:number \"7.17.0\"
              :build_flavor \"default\"
              :build_type \"docker\"}
    :tagline \"You Know, for Search\"}

   OpenSearch response example:
   {:name \"node-1\"
    :cluster_name \"opensearch\"
    :version {:distribution \"opensearch\"
              :number \"2.19.0\"
              :build_type \"docker\"}
    :tagline \"The OpenSearch Project: https://opensearch.org/\"}"
  [cluster-info]
  (let [version-info (:version cluster-info)
        version-number (:number version-info)
        distribution (or (:distribution version-info) "elasticsearch")
        parsed-version (parse-version version-number)]
    (cond
      ;; OpenSearch has a "distribution" field
      (= distribution "opensearch")
      {:engine :opensearch
       :version parsed-version}

      ;; Elasticsearch (no distribution field, or explicit "elasticsearch")
      :else
      {:engine :elasticsearch
       :version parsed-version})))

(s/defn verify-connection :- EngineInfo
  "Verify connection and detect engine type and version.

   Usage:
     (def conn (es-conn/connect {:host \"localhost\" :port 9200}))
     (verify-connection conn)
     ;; => {:engine :opensearch
     ;;     :version {:major 2 :minor 19 :patch 0}}

   This is useful for:
   1. Auto-detecting the engine type if not specified
   2. Validating the connection configuration
   3. Getting detailed version information"
  [conn :- ESConn]
  (-> conn
      get-cluster-info
      detect-engine))

(s/defn version-compare :- s/Int
  "Compare two version maps. Returns:
   - negative if v1 < v2
   - zero if v1 == v2
   - positive if v1 > v2"
  [v1 :- VersionInfo
   v2 :- VersionInfo]
  (let [major-cmp (compare (:major v1) (:major v2))]
    (if (not= 0 major-cmp)
      major-cmp
      (let [minor-cmp (compare (:minor v1) (:minor v2))]
        (if (not= 0 minor-cmp)
          minor-cmp
          (compare (or (:patch v1) 0) (or (:patch v2) 0)))))))

(s/defn version>=? :- s/Bool
  "Check if version v1 >= v2"
  [v1 :- VersionInfo
   v2 :- VersionInfo]
  (>= (version-compare v1 v2) 0))

(s/defn version<? :- s/Bool
  "Check if version v1 < v2"
  [v1 :- VersionInfo
   v2 :- VersionInfo]
  (< (version-compare v1 v2) 0))
