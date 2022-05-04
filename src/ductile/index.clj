(ns ductile.index
  (:refer-clojure :exclude [get])
  (:require [cemerick.uri :as uri]
            [ductile.conn :refer [make-http-opts safe-es-read]]
            [ductile.schemas :refer [ESConn RolloverConditions CatIndices]]
            [schema.core :as s]
            [schema-tools.core :as st]))

(s/defn index-uri :- s/Str
  "make an index uri from a host and an index name"
  [uri :- s/Str
   index-name :- s/Str]
  (format "%s/%s"
          uri
          (uri/uri-encode index-name)))

(s/defn template-uri :- s/Str
  "make a template uri from a host and a template name"
  [uri :- s/Str
   template-name :- s/Str]
  "make a template uri from a host and a template name"
  (format "%s/_template/%s"
          uri
          (uri/uri-encode template-name)))

(s/defn rollover-uri :- s/Str
  "make a rollover uri from a host and an index name"
  ([uri alias] (rollover-uri uri alias nil false))
  ([uri :- s/Str
    alias :- s/Str
    new-index-name :- (s/maybe s/Str)
    dry_run :- (s/maybe s/Bool)]
   (cond-> (str (index-uri uri alias) "/_rollover")
        new-index-name (str "/" (uri/uri-encode new-index-name))
        dry_run (str "?dry_run"))))

(s/defn refresh-uri :- s/Str
  "make a refresh uri from a host, and optionally an index name"
  [uri :- s/Str
   index-name :- (s/maybe s/Str)]
  (str uri
       (when index-name
         (str "/" (uri/uri-encode index-name)))
       "/_refresh"))

(s/defn index-exists? :- s/Bool
  "check if the supplied ES index exists"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str]
  (= 200
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

(s/defn update-settings!
  "update an ES index settings"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str
   settings :- s/Any]
  (-> (make-http-opts conn {} [] settings nil)
      (assoc :method :put
             :url (str (index-uri uri index-name) "/_settings"))
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
              :url (-> (index-uri uri index-name)
                       (uri/uri "_mapping" doc-type)
                       str))
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
             :url (str (index-uri uri index-name) "/_open"))
      request-fn
      safe-es-read))

(s/defn close!
  "close an index"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-name :- s/Str]
  (-> (make-http-opts conn {})
      (assoc :method :post
             :url (str (index-uri uri index-name) "/_close"))
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
            (update :docs.count read-string)
            (update :docs.deleted read-string)
            (update :pri read-string)
            (update :rep read-string))
       cat-res))

(s/defn cat-indices :- (s/maybe CatIndices)
  "perform a _cat/indices request"
  [{:keys [uri request-fn] :as conn} :- ESConn]
  (let [url (str uri "/_cat/indices?format=json&pretty=true&v=true")]
    (-> (make-http-opts conn {})
        (assoc :method :get
               :url url)
        request-fn
        safe-es-read
        format-cat)))
