(ns ductile.document
  (:require [cemerick.uri :as uri]
            [cheshire.core :as json]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [ductile.conn :as conn]
            [ductile.pagination :as pagination]
            [ductile.query :as q]
            [ductile.schemas :refer [CRUDOptions
                                     ESAggs
                                     ESConn
                                     ESQuery
                                     UpdateByQueryParams]]
            [schema-tools.core :as st]
            [schema.core :as s])
  (:import java.io.ByteArrayInputStream))

(def default-limit 1000)
(def default-retry-on-conflict 5)

(defn index-doc-uri
  "make an uri for document index"
  ([uri index-name doc-type id]
   (str (uri/uri uri
                 (uri/uri-encode index-name)
                 (or (not-empty doc-type) "_doc")
                 (uri/uri-encode id)))))

(def delete-doc-uri
  "make an uri for doc deletion"
  index-doc-uri)

(def get-doc-uri
  "make an uri for doc retrieval"
  index-doc-uri)

(defn update-doc-uri
  "make an uri for document update"
  ([uri index-name id]
   (str
    (uri/uri uri
             (uri/uri-encode index-name)
             "_update"
             (uri/uri-encode id))))
  ([uri index-name doc-type id]
   (if doc-type
     (-> (index-doc-uri uri index-name doc-type id)
         (str "/_update"))
     (update-doc-uri uri index-name id))))

(defn bulk-uri
  "make an uri for bulk action"
  [uri]
  (str (uri/uri uri "_bulk")))

(defn search-uri
  "make an uri for search action"
  [uri index-name]
  (cond-> uri
    index-name (str "/" (uri/uri-encode index-name))
    :else (str "/_search")))

(defn count-uri
  "make an uri for search action"
  [uri index-name]
  (str
   (uri/uri uri
            (uri/uri-encode index-name)
            "_count")))

(def ^:private special-operation-keys
  "all operations fields for a bulk operation"
  [:_doc_as_upsert
   :_index
   :_type
   :_id
   :_retry_on_conflict
   :_routing
   :_percolate
   :_parent
   :_script
   :_script_params
   :_scripted_upsert
   :_timestamp
   :_ttl
   :_version
   :_version_type])

(defn byte-size
  "Count the size of the given string in bytes."
  [^String s]
  (when s
    (count (.getBytes s))))

(defn format-bulk-doc-fn
  "helper to prepare a bulk operation"
  [op-type]
  (fn [doc]
    {op-type (select-keys doc special-operation-keys)}))

(defn format-bulk-docs
  "generates the content for a bulk operation"
  [op-type documents]
  (let [format-doc (format-bulk-doc-fn op-type)
        operations (map format-doc documents)
        documents  (map #(apply dissoc % special-operation-keys) documents)]
    (case op-type
      :delete [operations]
      :update (map vector
                   operations
                   (map #(array-map :doc %)
                        documents))
      (map vector operations documents))))

(defn partition-json-ops
  "Return a lazy sequence of lists of ops whose size is less than max-size.
   If a json-op exceeds the max size, it is included in a list of one element."
  [json-ops max-size]
  (let [ops-with-size (map (fn [op]
                             [(byte-size op) op])
                           json-ops) ;; [[12 op1] [53 op2] ...]
        groups (reduce (fn [acc [size op]]
                         (let [[[size-group group] & xs] acc
                               new-size (+ size-group size)]
                           ;; Can the new element be appended to
                           ;; the current group ?
                           ;; add at least one element
                           (if (or (empty? group)
                                   (<= new-size max-size))
                             (cons [new-size (conj group op)] xs)
                             (cons [size [op]] acc))))
                       [[0 []]] ;; initial group
                       ops-with-size)]
    (reverse (map second groups))))

(defn string->input-stream
  [^String s]
  (-> s
      (.getBytes)
      (ByteArrayInputStream.)))

(defn ^:private bulk-post-docs
  [json-ops
   {:keys [uri request-fn] :as conn}
   opts]
  (let [bulk-body (-> json-ops
                      (interleave (repeat "\n"))
                      string/join
                      string->input-stream)]
    (-> (conn/make-http-opts conn
                             opts
                             [:refresh]
                             nil
                             bulk-body)
        (assoc :method :post
               :url (bulk-uri uri))
        request-fn
        conn/safe-es-read
        conn/safe-es-bulk-read)))

(s/defschema BulkOps
  (s/enum :create :index :update :delete))

(s/defschema BulkActions
  {BulkOps [(s/pred map?)]})

(defn format-bulk-res
  [bulk-res-list]
  {:took (apply + (map :took bulk-res-list))
   :errors (some? (some (comp true? :errors) bulk-res-list))
   :items (mapcat :items bulk-res-list)})

(s/defn bulk
  "Bulk actions on ES"
  ([conn :- ESConn
    actions :- BulkActions
    opts :- CRUDOptions]
   (bulk conn actions opts nil))
  ([{:keys [version] :as conn} :- ESConn
    actions :- BulkActions
    opts :- CRUDOptions
    max-size :- (s/maybe s/Int)]
   (let [ops (mapcat
              (fn [[op-type docs]]
                (format-bulk-docs op-type
                                  (cond->> docs
                                    (= version 7) (map #(dissoc % :_type)))))
              actions)
         json-ops (map (fn [xs]
                         (->> xs
                              (map #(json/generate-string % {:pretty false}))
                              (string/join "\n")))
                       ops)
         json-ops-groups (if max-size
                           (partition-json-ops json-ops max-size)
                           [json-ops])
         bulk-res-list  (doall
                         (map #(bulk-post-docs % conn opts)
                              json-ops-groups))]
     (format-bulk-res bulk-res-list))))

(s/defn bulk-create-docs
  "create multiple documents on ES"
  ([conn :- ESConn
    docs :- [(s/pred map?)]
    opts :- CRUDOptions]
   (bulk-create-docs conn docs opts nil))
  ([conn :- ESConn
    docs :- [(s/pred map?)]
    opts :- CRUDOptions
    max-size :- (s/maybe s/Int)]
   (bulk conn
         {:create docs}
         opts
         max-size)))

(s/defn bulk-index-docs
  "index multiple documents on ES"
  ([conn :- ESConn
    docs :- [(s/pred map?)]
    opts :- CRUDOptions]
   (bulk-index-docs conn docs opts nil))
  ([conn :- ESConn
    docs :- [s/Any]
    opts :- CRUDOptions
    max-size :- (s/maybe s/Int)]
   (bulk conn
         {:index docs}
         opts
         max-size)))

(s/defn bulk-update-docs
  "update multiple documents on ES"
  ([conn :- ESConn
    docs :- [(st/open-schema
              {:_id s/Str
               :_index s/Str})]
    opts :- CRUDOptions]
   (bulk-update-docs conn docs opts nil))
  ([conn :- ESConn
    docs :- [s/Any]
    opts :- CRUDOptions
    max-size :- (s/maybe s/Int)]
   (bulk conn
         {:update docs}
         opts
         max-size)))

(s/defn bulk-delete-docs
  "delete multiple documents on ES"
  ([conn :- ESConn
    docs :- [(st/open-schema
              {:_id s/Str
               :_index s/Str})]
    opts :- CRUDOptions]
   (bulk-delete-docs conn docs opts nil))
  ([conn :- ESConn
    docs :- [s/Any]
    opts :- CRUDOptions
    max-size :- (s/maybe s/Int)]
   (bulk conn
         {:delete docs}
         opts
         max-size)))

(s/defn get-doc
  "get a document on es and return only the source"
  ([{:keys [uri request-fn version] :as conn} :- ESConn
    index-name :- s/Str
    doc-type :- (s/maybe s/Str)
    id :- s/Str
    opts :- CRUDOptions]
   (-> (conn/make-http-opts conn opts [:_source])
       (assoc :method :get
              :url (get-doc-uri uri
                                index-name
                                (when (= version 5) doc-type)
                                id))
       request-fn
       conn/safe-es-read
       :_source))
  ([conn :- ESConn
    index-name :- s/Str
    id :- s/Str
    opts :- CRUDOptions]
   (get-doc conn index-name nil id opts)))

(s/defn ^:private index-doc-internal
  [{:keys [uri request-fn version] :as conn} :- ESConn
   index-name :- s/Str
   doc-type :- (s/maybe s/Str)
   doc :- s/Any
   {:keys [mk-id]
    :or {mk-id :id}
    :as opts} :- CRUDOptions]
  (let [doc-id (mk-id doc)
        valid-opts (cond-> [:refresh]
                     ;; es5 does not allow op_type=create when no id is provided
                     ;; https://github.com/elastic/elasticsearch/issues/21535#issuecomment-260467699
                     doc-id (conj :op_type))
        uri (index-doc-uri uri
                           index-name
                           (when (= version 5) doc-type)
                           doc-id)]
    (-> (conn/make-http-opts conn opts valid-opts doc nil)
        (assoc :method :post
               :url uri)
        request-fn
        conn/safe-es-read)))

(s/defn index-doc
  "index a document on es return the indexed document"
  ([conn :- ESConn
    index-name :- s/Str
    doc-type :- (s/maybe s/Str)
    doc :- s/Any
    opts :- CRUDOptions]
   (index-doc-internal conn index-name doc-type doc opts))
  ([conn :- ESConn
    index-name :- s/Str
    doc :- s/Any
    opts :- CRUDOptions]
   (index-doc-internal conn index-name nil doc opts)))

(s/defn create-doc
  "create a document on es return the created document"
  ([conn :- ESConn
    index-name :- s/Str
    doc :- s/Any
    opts :- CRUDOptions]
   (create-doc conn index-name nil doc opts))
  ([conn :- ESConn
    index-name :- s/Str
    doc-type :- (s/maybe s/Str)
    doc :- s/Any
    opts :- CRUDOptions]
   (index-doc-internal conn
                       index-name
                       doc-type
                       doc
                       (assoc opts :op_type "create"))))

(defn ^:private update-doc-raw
  [uri conn doc request-fn opts]
  (-> (conn/make-http-opts conn
                           (into {:_source true
                                  :retry_on_conflict default-retry-on-conflict}
                                 opts)
                           [:_source :retry_on_conflict :refresh]
                           {:doc doc}
                           nil)
      (assoc :method :post
             :url uri)
      request-fn
      conn/safe-es-read
      (get-in [:get :_source])))

(s/defn update-doc
  "update a document on es return the updated document"
  ([{:keys [uri request-fn version] :as conn} :- ESConn
    index-name :- s/Str
    doc-type :- (s/maybe s/Str)
    id :- s/Str
    doc :- s/Any
    opts :- CRUDOptions]
   (-> (update-doc-uri uri
                       index-name
                       (when (= version 5) doc-type)
                       id)
       (update-doc-raw conn doc request-fn opts)))
  ([conn :- ESConn
    index-name :- s/Str
    id :- s/Str
    doc :- s/Any
    opts :- CRUDOptions]
   (update-doc conn index-name nil id doc opts)))

(s/defn delete-doc
  "delete a document on es, returns boolean"
  ([conn :- ESConn
    index-name :- s/Str
    id :- s/Str
    opts :- CRUDOptions]
   (delete-doc conn index-name nil id opts))
  ([{:keys [uri request-fn version] :as conn} :- ESConn
    index-name :- s/Str
    doc-type :- (s/maybe s/Str)
    id :- s/Str
    opts :- CRUDOptions]
   (-> (conn/make-http-opts conn opts [:refresh])
       (assoc :method :delete
              :url (delete-doc-uri uri
                                   index-name
                                   (when (= version 5) doc-type)
                                   id))
       request-fn
       conn/safe-es-read
       :result
       (= "deleted"))))

(s/defn delete-by-query-uri
  [uri index-names]
  (let [index (uri/uri-encode (string/join "," index-names))]
    (str (uri/uri uri index "_delete_by_query"))))

(s/defn delete-by-query
  "delete all documents that match a query in an index"
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-names :- [s/Str]
   q :- ESQuery
   opts :- CRUDOptions]
  (-> (conn/make-http-opts conn
                           opts
                           [:refresh :wait_for_completion]
                           {:query q}
                           nil)
      (assoc :method :post
             :url (delete-by-query-uri uri index-names))
      request-fn
      conn/safe-es-read))

(s/defn update-by-query-uri
  [uri index-names]
  (let [index (uri/uri-encode (string/join "," index-names))]
    (str (uri/uri uri index "_update_by_query"))))

(s/defn update-by-query
  "Performs an update on every document in the data stream or index without modifying
  the source, which is useful for picking up mapping changes."
  [{:keys [uri request-fn] :as conn} :- ESConn
   index-names :- [s/Str]
   form-params :- (s/maybe UpdateByQueryParams)
   opts :- CRUDOptions]
  (-> (conn/make-http-opts conn
                           opts
                           [:refresh :wait_for_completion :conflicts]
                           (or form-params {})
                           {})
      (assoc :method :post
             :url (update-by-query-uri uri index-names))
      request-fn
      conn/safe-es-read))

(defn sort-params
  [sort_by sort_order]
  (let [sort-fields
        (map (fn [field]
               (let [[field-name field-order] (string/split field #":")]
                 {field-name
                  {:order (keyword (or field-order sort_order))}}))
             (string/split (name sort_by) #","))]

    ;; FIXME hash map loses ordering, "sort" accepts a list
    {:sort (into {} sort-fields)}))

(defn sort-params-ext
  [sort_by_ext default-sort_order]
  {:sort (mapv (fn [{:keys [op] :as params}]
                 (case op
                   :field (let [{:keys [field-name sort_order]} params
                                order (or default-sort_order sort_order)]
                            (assert (keyword? order) (pr-str order))
                            (assert (string? field-name) (pr-str field-name))
                            {field-name {:order order}})
                   :remap-strings-to-numbers (let [{:keys [field-name mapping sort_order]} params
                                                   order (or default-sort_order sort_order)]
                                               (assert (string? field-name) (pr-str field-name))
                                               (assert (not (some #{"'"} field-name)) (pr-str field-name))
                                               (assert (keyword? order) (pr-str order))
                                               (assert (seq mapping) (pr-str mapping))
                                               ;; https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-sort-context.html
                                               {:_script
                                                {:type "number"
                                                 :script {:lang "painless"
                                                          ;; https://www.elastic.co/guide/en/elasticsearch/painless/5.6/_operators.html#_elvis
                                                          :inline (format "params[doc['%s']] ?: 0" field-name)
                                                          :params mapping}
                                                 :order }})))
               sort_by_ext)})

(defn params->pagination
  [{:keys [sort_by sort_by_ext sort_order offset limit search_after]
    :or {sort_order :asc
         limit pagination/default-limit} :as opt}]
  (assert (not (and sort_by sort_by_ext))
          "Cannot provide both :sort_by and :sort_by_ext")
  (merge
   {}
   (when sort_by
     (sort-params sort_by sort_order))
   (when sort_by_ext
     (sort-params-ext sort_by_ext sort_order))
   (when limit
     {:size limit})
   (when (and offset
              (not search_after))
     {:from offset})
   (when search_after
     {:from 0
      :search_after search_after})))

(defn generate-search-params
  [query aggs params]
  (cond-> (into (params->pagination params)
                (select-keys params [:sort :_source :track_total_hits]))
    query (assoc :query query)
    aggs (assoc :aggs aggs)))

(s/defn count-docs
  "Count documents on ES matching given query."
  ([{:keys [uri request-fn] :as conn} :- ESConn
    index-name :- s/Str
    query :- (s/maybe ESQuery)]
   (-> (conn/make-http-opts conn
                            {}
                            []
                            (when query
                              {:query query})
                            nil)
       (assoc :method :post
              :url (count-uri uri index-name))
       request-fn
       conn/safe-es-read
       :count))
  ([es-conn :- ESConn
    index-name :- s/Str]
   (count-docs es-conn index-name nil)))

(defn ^:private result-data
  [res full-hits?]
  (cond->> (-> res :hits :hits)
    (not full-hits?) (map :_source)))

(defn ^:private pagination-params
  [{:keys [hits]}
   {:keys [from size search_after]}]
  {:offset from
   :limit size
   :sort (-> hits :hits last :sort)
   :search_after search_after
   :total-hits (or (get-in hits [:total :value])
                   (:total hits) ;; compatibility with 5.x
                   0)})

(defn ^:private format-result
  [{:keys [aggregations] :as res}
   es-params
   full-hits?]
  (cond-> (pagination/response (result-data res full-hits?)
                               (pagination-params res es-params))
    aggregations (assoc :aggs aggregations)))

(s/defn query
  "Search for documents on ES using any query. Performs aggregations when specified."
  ([{:keys [uri request-fn] :as conn} :- ESConn
    index-name :- (s/maybe s/Str)
    q :- (s/maybe ESQuery)
    aggs :- (s/maybe ESAggs)
    {:keys [full-hits?] :as opts} :- s/Any]
   (let [search-params (generate-search-params q aggs opts)
         res (-> (conn/make-http-opts conn
                                      {}
                                      []
                                      search-params
                                      nil)
                 (assoc :method :post
                        :url (search-uri uri index-name))
                 request-fn
                 conn/safe-es-read)]
     (log/debug "search:" search-params)
     (format-result res search-params full-hits?)))
  ([conn index-name q opts]
   (query conn index-name q nil opts)))

(s/defn search-docs
  "Search for documents on ES using a query string search.  Also applies a filter map, converting
   the values in the all-of into must match terms."
  [conn :- ESConn
   index-name :- (s/maybe s/Str)
   es-query :- (s/maybe ESQuery)
   all-of :- (s/maybe {s/Any s/Any})
   opts :- s/Any]
  (let [bool-query (q/filter-map->terms-query all-of es-query)]
    (query conn index-name bool-query opts)))
