(ns ductile.settings
  (:require [ductile.uri :as uri]
            [ductile.conn :refer [make-http-opts safe-es-read]]
            [ductile.schemas :refer [ESConn ESSettings]]
            [schema.core :as s]
            [clojure.edn :as edn]
            [clojure.walk :as walk]
            [clojure.string :as string]))

(defn coerce [^String st]
  (try
    (let [v (edn/read-string st)]
      (if (symbol? v) st v))
    (catch Exception _ st)))

(defn deep-merge [m1 m2]
  (if (and (map? m1) (map? m2))
    (merge-with deep-merge m1 m2)
    (or m2 m1)))

(defn merge-defaults [settings]
  (walk/postwalk
   (fn [f]
     (if (string? f)
       (coerce f)
       f))
   (reduce-kv
    (fn [acc index {:keys [defaults settings]}]
      (if (string/starts-with? (name index) ".")
        acc
        (assoc acc index (deep-merge settings defaults))))
    {}
    settings)))

(s/defn fetch :- ESSettings
  "Extract settings of an `index` from ES cluster. If explicit `index` is not provided - uses `\"_all\"` as a target.
   Result is a hash map of `index` -> `settings`, including defaults."
  ([conn :- ESConn] (fetch conn "_all"))
  ([{:keys [uri request-fn] :as conn} :- ESConn
    index :- s/Str]
   (-> (make-http-opts conn)
       (assoc :query-params {:include_defaults true
                             :ignore_unavailable true}
              :method :get
              :url (uri/uri uri (uri/uri-encode index) "_settings"))
       (request-fn)
       (safe-es-read)
       (merge-defaults))))
