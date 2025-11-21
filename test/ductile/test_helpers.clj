(ns ductile.test-helpers
  (:require [clojure.test :refer [testing]]
            [ductile.conn :as es-conn]))

(def basic-auth-opts
  {:type :basic-auth
   :params {:user "elastic" :pwd "ductile"}})

(def opensearch-auth-opts
  ;; Security disabled for testing
  {})

(defn engine-port
  "Map engine/version pairs to their Docker container ports"
  [engine version]
  (case [engine version]
    [:elasticsearch 7] 9207
    [:opensearch 2]    9202
    [:opensearch 3]    9203
    ;; Default fallback
    (+ 9200 version)))

(defn engine-auth
  "Get auth options for the given engine"
  [engine]
  (case engine
    :elasticsearch basic-auth-opts
    :opensearch opensearch-auth-opts
    basic-auth-opts))

(defn connect
  "Connect to an engine with given version.
   Supports both old API (version only) and new API (engine, version)."
  ([version auth-opts]
   ;; Backward compatibility: assume elasticsearch if only version provided
   (es-conn/connect
    (cond-> {:host "localhost"
             :port (+ 9200 version)
             :version version
             :engine :elasticsearch}
      (seq auth-opts) (assoc :auth auth-opts))))
  ([engine version auth-opts]
   (es-conn/connect
    (cond-> {:host "localhost"
             :port (engine-port engine version)
             :version version
             :engine engine}
      (seq auth-opts) (assoc :auth auth-opts)))))

(def engine-version-pairs
  "Default engine/version pairs for testing.
   Set DUCTILE_TEST_ENGINES env var to filter, e.g.:
   - 'es' for Elasticsearch only
   - 'os' for OpenSearch only
   - 'all' or unset for both"
  (let [test-env (or (System/getenv "DUCTILE_TEST_ENGINES") "all")
        all-pairs [[:elasticsearch 7]
                   [:opensearch 2]
                   [:opensearch 3]]]
    (case test-env
      "es" (filter (fn [[engine _]] (= engine :elasticsearch)) all-pairs)
      "os" (filter (fn [[engine _]] (= engine :opensearch)) all-pairs)
      "all" all-pairs
      all-pairs)))

(defmacro for-each-es-version [msg clean & body]
  "For each configured engine/version pair:
- init a connection
- expose anaphoric `engine`, `version` and `conn` to use in body
- wrap body with a `testing` block with `msg` formatted with engine and version
- call `clean` fn if not `nil` before and after body.

Backward compatible: `version` is still available for existing tests."
  {:style/indent 2}
  `(doseq [[engine# version#] engine-version-pairs]
     (let [~'engine engine#
           ~'version version#
           ~'conn (connect engine# version# (engine-auth engine#))
           clean-fn# ~clean]
       (try
         (testing (format "%s (%s version: %s)" ~msg (name engine#) version#)
           (when clean-fn#
             (clean-fn#))
           ~@body
           (when clean-fn#
             (clean-fn#)))
         (finally (es-conn/close ~'conn))))))
