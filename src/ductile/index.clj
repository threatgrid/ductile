(ns ductile.index
  (:refer-clojure :exclude [get])
  (:require
   [clojure.edn :as edn]
   [clojure.string :as string]
   [clojure.walk :as walk]
   [ductile.conn :refer [make-http-opts safe-es-read]]
   [ductile.schemas :refer [CatIndices ESConn RolloverConditions ESSettings Policy]]
   [ductile.uri :as uri]
   [schema-tools.core :as st]
   [schema.core :as s]))

(s/defn index-uri :- s/Str
  "make an index uri from a host and an index name"
  [uri :- s/Str
   index-name :- s/Str]
  (uri/uri uri (uri/uri-encode index-name)))

(s/defn template-uri :- s/Str
  "make a template uri from a host and a template name"
  [uri :- s/Str
   template-name :- s/Str]
  "make a template uri from a host and a template name"
  (uri/uri uri "_template" (uri/uri-encode template-name)))

(s/defn index-template-uri :- s/Str
  "make an index template uri from a host and a template name"
  [uri :- s/Str
   template-name :- s/Str]
  "make an index template uri from a host and a template name"
  (uri/uri uri "_index_template" (uri/uri-encode template-name)))

(s/defn rollover-uri :- s/Str
  "make a rollover uri from a host and an index name"
  ([uri alias] (rollover-uri uri alias nil false))
  ([uri :- s/Str
    alias :- s/Str
    new-index-name :- (s/maybe s/Str)
    dry-run :- (s/maybe s/Bool)]
   (cond-> (uri/uri (index-uri uri alias) "_rollover" (uri/uri-encode new-index-name))
     dry-run (str "?dry_run"))))

(s/defn refresh-uri :- s/Str
  "make a refresh uri from a host, and optionally an index name"
  [uri :- s/Str
   index-name :- (s/maybe s/Str)]
  (uri/uri uri (uri/uri-encode index-name) "_refresh"))

(s/defn policy-uri
  "make a policy uri from a host, and a policy name"
  [uri :- s/Str
   policy-name :- s/Str]
  (uri/uri uri "_ilm/policy" (uri/uri-encode policy-name)))

(s/defn data-stream-uri
  "make a datastral uri from a host, and a data stream name"
  [uri :- s/Str
   data-stream-name :- s/Str]
  (uri/uri uri "_data_stream" (uri/uri-encode data-stream-name)))

(s/defn create-data-stream!
  [{:keys [uri version request-fn] :as conn} :- ESConn
   data-stream-name :- s/Str]
  (when (< version 7)
    (throw (ex-info "Cannot create datastream for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn)
      (assoc :method :put
             :url (data-stream-uri uri data-stream-name))
      request-fn
      safe-es-read))

(s/defn delete-data-stream!
  [{:keys [uri version request-fn] :as conn} :- ESConn
   data-stream-name :- s/Str]
  (when (< version 7)
    (throw (ex-info "Cannot delete data stream for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn)
      (assoc :method :delete
             :url (data-stream-uri uri data-stream-name))
      request-fn
      safe-es-read))

(s/defn get-data-stream
  [{:keys [uri version request-fn] :as conn} :- ESConn
   data-stream-name :- s/Str]
  (when (< version 7)
    (throw (ex-info "Cannot get data stream for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn)
      (assoc :method :get
             :url (data-stream-uri uri data-stream-name))
      request-fn
      safe-es-read))

(s/defn create-policy!
  [{:keys [uri version request-fn] :as conn} :- ESConn
   policy-name :- s/Str
   policy :- Policy]
  (when (< version 7)
    (throw (ex-info "Cannot create policiy for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn
                      {}
                      []
                      {:policy policy}
                      nil)
      (assoc :method :put
             :url (policy-uri uri policy-name))
      request-fn
      safe-es-read))

(s/defn delete-policy!
  [{:keys [uri version request-fn] :as conn} :- ESConn
   policy-name :- s/Str]
  (when (< version 7)
    (throw (ex-info "Cannot delete policiy for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn)
      (assoc :method :delete
             :url (policy-uri uri policy-name))
      request-fn
      safe-es-read))

(s/defn get-policy
  [{:keys [uri version request-fn] :as conn} :- ESConn
   policy-name :- s/Str]
  (when (< version 7)
    (throw (ex-info "Cannot get policiy for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn)
      (assoc :method :get
             :url (policy-uri uri policy-name))
      request-fn
      safe-es-read))

(s/defn index-exists? :- s/Bool
  "check if the supplied ES index exists"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str]
  (not= 404
        (-> (make-http-opts conn {})
            (assoc :method :head
                   :url (index-uri uri index-name))
            request-fn
            :status)))

(s/defn create!
  "create an index"
  [{:keys [uri request-fn] :as conn} :- ESConn
    index-name :- s/Str
    settings :- s/Any]
  (-> (make-http-opts conn
                      {}
                      []
                      settings
                      nil)
      (assoc :method :put
             :url (index-uri uri index-name))
      request-fn
      safe-es-read))

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
      ;; NOTE by convention Elasticsearch indices
      ;;      with a leading dot in the name
      ;;      considered internal.
      (if (string/starts-with? (name index) ".")
        acc
        (assoc acc index (deep-merge settings defaults))))
    {}
    settings)))

(s/defn get-settings :- ESSettings
  "Extract settings of an `index` from ES cluster. If explicit `index` is not provided - uses `\"_all\"` as a target.
   Result is a hash map of `index` -> `settings`, including defaults."
  ([conn :- ESConn] (get-settings conn "_all"))
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

(s/defn update-settings!
  "update an ES index settings"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str
   settings :- s/Any]
  (-> (make-http-opts conn {} [] settings nil)
      (assoc :method :put
             :url (uri/uri (index-uri uri index-name) "_settings"))
      request-fn
      safe-es-read))

(s/defn update-mappings!
  "Update an ES index mapping. takes a mappings map
  from field names to mapping types."
  ([{:keys [uri request-fn] :as conn} :- ESConn
    index-name :- s/Str
    doc-type :- (s/maybe s/Str)
    mappings :- (s/pred map?)]
   (-> (make-http-opts conn {} [] mappings nil)
       (assoc :method :put
              :url (uri/uri (index-uri uri index-name) "_mapping" (uri/uri-encode doc-type)))
       request-fn
       safe-es-read))
  ([conn :- ESConn
    index-name :- s/Str
    mappings :- (s/pred map?)]
   (update-mappings! conn index-name nil mappings)))

(s/defn get
  "get an index"
  [{:keys [uri request-fn] :as conn} :- ESConn
    index-name :- s/Str]
   (-> (make-http-opts conn {})
       (assoc :method :get
              :url (index-uri uri index-name))
       request-fn
       safe-es-read))

(s/defn delete!
  "delete indexes using a wildcard"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-wildcard :- s/Str]
  (-> (make-http-opts conn {})
      (assoc :method :delete
             :url (index-uri uri index-wildcard))
      request-fn
      safe-es-read))

(s/defn get-template
  "get an index template"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str]
  (-> (make-http-opts conn {})
      (assoc :method :get
             :url (template-uri uri index-name))
      request-fn
      safe-es-read))

(s/defn create-template!
  "create an index template, update if already exists"
  ([{:keys [uri request-fn version] :as conn} :- ESConn
    template-name :- s/Str
    index-config
    index-patterns :- [s/Str]]
   (let [template (cond-> index-config
                    (>= version 6) (assoc :index_patterns index-patterns)
                    (= version 5) (assoc :template (first index-patterns)))]
     (-> (make-http-opts conn {} nil template nil)
         (assoc :method :put
                :url (template-uri uri template-name))
         request-fn
         safe-es-read)))
  ([conn :- ESConn
    template-name :- s/Str
    index-config]
   (create-template! conn
                     template-name
                     index-config
                     [(str template-name "*")])))

(s/defn delete-template!
  "delete a template"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str]
  (-> (make-http-opts conn {})
      (assoc :method :delete
             :url (template-uri uri index-name))
      request-fn
      safe-es-read))

(s/defn get-index-template
  "get an index template"
  [{:keys [uri request-fn version] :as conn} :- ESConn
   index-name :- s/Str]
  (when (< version 7)
    (throw (ex-info "Cannot get index-template for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn {})
      (assoc :method :get
             :url (index-template-uri uri index-name))
      request-fn
      safe-es-read))

(s/defn create-index-template!
  "create an index template, update if already exists"
  [{:keys [uri request-fn version] :as conn} :- ESConn
   template-name :- s/Str
   template]
  (when (< version 7)
    (throw (ex-info "Cannot create index-template for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn {} nil template nil)
      (assoc :method :put
             :url (index-template-uri uri template-name))
      request-fn
      safe-es-read))

(s/defn delete-index-template!
  "delete a template"
  [{:keys [uri request-fn version] :as conn} :- ESConn
   index-name :- s/Str]
  (when (< version 7)
    (throw (ex-info "Cannot delete index-template for Elasticsearch version < 7" conn)))
  (-> (make-http-opts conn {})
      (assoc :method :delete
             :url (index-template-uri uri index-name))
      request-fn
      safe-es-read))

(s/defn refresh!
  "refresh an index"
  ([es-conn] (refresh! es-conn nil))
  ([{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- (s/maybe s/Str)]
   (-> (make-http-opts conn {})
       (assoc :method :post
              :url (refresh-uri uri index-name))
       request-fn
       safe-es-read)))

(s/defn open!
  "open an index"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str]
  (-> (make-http-opts conn {})
      (assoc :method :post
             :url (uri/uri (index-uri uri index-name) "_open"))
      request-fn
      safe-es-read))

(s/defn close!
  "close an index"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str]
  (-> (make-http-opts conn {})
      (assoc :method :post
             :url (uri/uri (index-uri uri index-name) "_close"))
      request-fn
      safe-es-read))

(s/defn rollover!
  "run a rollover query on an alias with given conditions"
  ([es-conn alias conditions]
   (rollover! es-conn alias conditions {}))
  ([{:keys [uri request-fn] :as conn} :- ESConn
    alias :- s/Str
    conditions :- RolloverConditions
    {:keys [dry_run
            new-index-settings
            new-index-name]} :- (st/open-schema
                                 (st/optional-keys
                                  {:new-index-settings (s/pred map?)
                                   :new-index-name (s/maybe s/Str)
                                   :dry_run s/Bool}))]
   (let [url (rollover-uri uri alias new-index-name dry_run)
         rollover-params (cond-> {:conditions conditions}
                           new-index-settings (assoc :settings new-index-settings))]
     (-> (make-http-opts conn
                         {}
                         []
                         rollover-params
                         nil)
         (assoc :method :post
                :url url)
         request-fn
         safe-es-read))))

(defn ^:private format-cat
  [cat-res]
  (map #(-> %
            (update :docs.count edn/read-string)
            (update :docs.deleted edn/read-string)
            (update :pri edn/read-string)
            (update :rep edn/read-string))
       cat-res))

(s/defn cat-indices :- (s/maybe CatIndices)
  "perform a _cat/indices request"
  [{:keys [uri request-fn] :as conn} :- ESConn]
  (let [url (str uri "/_cat/indices?format=json&v=true")]
    (-> (make-http-opts conn {})
        (assoc :method :get
               :url url)
        request-fn
        safe-es-read
        format-cat)))
