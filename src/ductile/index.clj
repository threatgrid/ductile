(ns ductile.index
  (:refer-clojure :exclude [get])
  (:require [cemerick.uri :as uri]
            [clj-http.client :as client]
            [ductile.conn :refer [make-http-opts safe-es-read]]
            [ductile.schemas :refer [ESConn RolloverConditions]]
            [schema.core :as s]))

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
    dry_run :- s/Bool]
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
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str]
  (not= 404
     (:status (client/head (index-uri uri index-name)
                           (make-http-opts cm)))))

(s/defn create!
  "create an index"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str
   settings :- s/Any]
  (safe-es-read
   (client/put (index-uri uri index-name)
               (make-http-opts cm
                                {}
                                []
                                settings
                                nil))))

(s/defn update-settings!
  "update an ES index settings"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str
   settings :- s/Any]
  (safe-es-read
   (client/put (str (index-uri uri index-name) "/_settings")
               (make-http-opts cm {} [] settings nil))))

(s/defn update-mappings!
  "Update an ES index mapping. takes a mappings map
  from field names to mapping types."
  [{:keys [uri cm] :as conn} :- ESConn
   index-name :- s/Str
   mappings :- {:properties {s/Any s/Any}}]
  (safe-es-read
   (client/put (str (index-uri uri index-name) "/_mapping")
               (make-http-opts cm {} [] mappings nil))))

(s/defn get
  "get an index"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str]
  (safe-es-read
   (client/get (index-uri uri index-name)
               (make-http-opts cm))))

(s/defn delete!
  "delete indexes using a wildcard"
  [{:keys [uri cm]} :- ESConn
   index-wildcard :- s/Str]
  (safe-es-read
   (client/delete (index-uri uri index-wildcard)
                  (make-http-opts  cm))))

(s/defn get-template
  "get an index template"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str]
  (safe-es-read
   (client/get (template-uri uri index-name)
               (make-http-opts cm))))

(s/defn create-template!
  "create an index template, update if already exists"
  ([{:keys [uri cm]} :- ESConn
    template-name :- s/Str
    index-config
    index-patterns :- [s/Str]]
  (let [template (assoc index-config :index_patterns index-patterns)]
    (safe-es-read
     (client/put (template-uri uri template-name)
                 (make-http-opts cm {} nil template nil)))))
  ([es-conn :- ESConn
    template-name :- s/Str
    index-config]
   (create-template! es-conn
                     template-name
                     index-config
                     [(str template-name "*")])))

(s/defn delete-template!
  "delete a template"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str]
  (safe-es-read
   (client/delete (template-uri uri index-name)
                  (make-http-opts cm))))

(s/defn refresh!
  "refresh an index"
  ([es-conn] (refresh! es-conn nil))
  ([{:keys [uri cm]} :- ESConn
    index-name :- (s/maybe s/Str)]
   (safe-es-read
    (client/post (refresh-uri uri index-name)
                 (make-http-opts cm)))))

(s/defn open!
  "open an index"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str]
  (safe-es-read
   (client/post (str (index-uri uri index-name) "/_open")
                (make-http-opts cm))))

(s/defn close!
  "close an index"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str]
  (safe-es-read
   (client/post (str (index-uri uri index-name) "/_close")
                (make-http-opts cm))))

(s/defn rollover!
  "run a rollover query on an alias with given conditions"
  ([es-conn alias conditions]
   (rollover! es-conn alias conditions {} nil false))
  ([{:keys [uri cm]} :- ESConn
    alias :- s/Str
    conditions :- RolloverConditions
    new-index-settings :- {s/Any s/Any}
    new-index-name :- (s/maybe s/Str)
    dry_run :- (s/maybe s/Bool)]
   (safe-es-read
    (client/post (rollover-uri uri alias new-index-name dry_run)
                 (make-http-opts cm
                                 {}
                                 []
                                 {:conditions conditions
                                  :settings new-index-settings}
                                 nil)))))
