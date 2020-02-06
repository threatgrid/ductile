(ns ductile.document
  (:require [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure.string :as string]
            [clojure.tools.logging :as log]
            [cemerick.uri :as uri]
            [ductile
             [conn :refer [default-opts
                           safe-es-read
                           safe-es-bulk-read
                           make-default-opts]]
             [schemas :refer [ESAggs ESConn ESQuery Refresh]]
             [pagination :as pagination]
             [query :as q]]
            [schema.core :as s]))

(def default-limit 1000)
(def default-retry-on-conflict 5)

(defn index-doc-uri
  "make an uri for document index"
  [uri index-name mapping id]
  (str (uri/uri uri (uri/uri-encode index-name) (uri/uri-encode mapping) (uri/uri-encode id))))

(def delete-doc-uri
  "make an uri for doc deletion"
  index-doc-uri)

(def get-doc-uri
  "make an uri for doc retrieval"
  index-doc-uri)

(defn update-doc-uri
  "make an uri for document update"
  [uri
   index-name
   mapping
   id
   retry-on-conflict]
  (str
   (assoc
     (uri/uri uri
              (uri/uri-encode index-name)
              (uri/uri-encode mapping)
              (uri/uri-encode id) "_update")
    :query {:retry_on_conflict
            retry-on-conflict})))

(defn bulk-uri
  "make an uri for bulk action"
  [uri]
  (str (uri/uri uri "_bulk")))

(defn search-uri
  "make an uri for search action"
  [uri index-name mapping]
  (cond-> uri
    index-name (str "/" (uri/uri-encode index-name))
    mapping (str "/" (uri/uri-encode mapping))
    true (str "/_search")))

(defn count-uri
  "make an uri for search action"
  [uri index-name mapping]
  (str (uri/uri uri (uri/uri-encode index-name) (uri/uri-encode mapping) "_count")))

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

(defn index-operation
  "helper to prepare a bulk insert operation"
  [doc]
  {"index" (select-keys doc special-operation-keys)})

(defn bulk-index
  "generates the content for a bulk insert operation"
  ([documents]
   (let [operations (map index-operation documents)
         documents  (map #(apply dissoc % special-operation-keys) documents)]
     (map vector operations documents))))

(s/defn get-doc
  "get a document on es and return only the source"
  [{:keys [uri cm]} :- ESConn index-name mapping id params]
  (-> (client/get (get-doc-uri uri
                               index-name
                               mapping
                               id)
                  (assoc (make-default-opts params)
                         :connection-manager cm))
      safe-es-read
      :_source))

(s/defn index-doc-internal
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str
   mapping :- s/Str
   {:keys [id] :as doc} :- s/Any
   {:keys [refresh op_type]}]
  (let [query-params (cond-> {}
                       refresh (assoc :refresh refresh)
                       op_type (assoc :op_type op_type))]
    (safe-es-read
     (client/put (index-doc-uri uri index-name mapping id)
                 (merge default-opts
                        {:form-params doc
                         :query-params query-params
                         :connection-manager cm})))
    doc))

(s/defn index-doc
  "index a document on es return the indexed document"
  [es-conn :- ESConn
   index-name :- s/Str
   mapping :- s/Str
   doc :- s/Any
   refresh? :- Refresh]
  (index-doc-internal es-conn index-name mapping doc {:refresh refresh?}))

(s/defn create-doc
  "create a document on es return the created document"
  [es-conn :- ESConn
   index-name :- s/Str
   mapping :- s/Str
   doc :- s/Any
   refresh? :- Refresh]
  (index-doc-internal es-conn index-name mapping doc {:refresh refresh?
                                                      :op_type "create"}))

(defn byte-size
  "Count the size of the given string in bytes."
  [^String s]
  (when s
    (count (.getBytes s))))

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

(defn- bulk-post-docs
  [json-ops
   {:keys [uri cm]}
   refresh?]
  (let [bulk-body (-> json-ops
                      (interleave (repeat "\n"))
                      string/join)]
    (-> (client/post (bulk-uri uri)
                     (merge default-opts
                            {:connection-manager cm
                             :query-params {:refresh refresh?}
                             :body bulk-body}))
        safe-es-read
        safe-es-bulk-read)))

(s/defn bulk-create-doc
  "create multiple documents on ES and return the created documents"
  ([conn :- ESConn
    docs :- [s/Any]
    refresh? :- Refresh]
   (bulk-create-doc conn docs refresh? nil))
  ([conn :- ESConn
    docs :- [s/Any]
    refresh? :- Refresh
    max-size :- (s/maybe s/Int)]
   (let [ops (bulk-index docs)
         json-ops (map (fn [xs]
                         (->> xs
                              (map #(json/generate-string % {:pretty false}))
                              (string/join "\n")))
                       ops)
         json-ops-groups (if max-size
                           (partition-json-ops json-ops max-size)
                           [json-ops])]
     (doseq [json-ops-group json-ops-groups]
       (bulk-post-docs json-ops-group conn refresh?))
     docs)))

(s/defn update-doc
  "update a document on es return the updated document"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str
   mapping :- s/Str
   id :- s/Str
   doc :- s/Any
   refresh? :- Refresh
   & [{:keys [retry-on-conflict]
       :or {retry-on-conflict
            default-retry-on-conflict}}]]
  (-> (client/post
       (update-doc-uri uri index-name mapping id retry-on-conflict)
       (merge default-opts
              {:form-params {:doc doc}
               :query-params {:refresh refresh?
                              :_source true}
               :connection-manager cm}))
      safe-es-read
      (get-in [:get :_source])))

(s/defn delete-doc
  "delete a document on es, returns boolean"
  [{:keys [uri cm]} :- ESConn
   index-name :- s/Str
   mapping :- s/Str
   id :- s/Str
   refresh? :- Refresh]
  (-> (client/delete (delete-doc-uri uri index-name mapping id)
                     (merge default-opts
                            {:query-params {:refresh refresh?}
                             :connection-manager cm}))
      safe-es-read
      :result
      (= "deleted")))

(s/defn delete-by-query-uri
  [uri index-names mapping]
  (let [index (string/join "," index-names)]
    (str (uri/uri uri
                  (uri/uri-encode index)
                  (uri/uri-encode mapping)
                  "_delete_by_query"))))

(s/defn delete-by-query
  "delete all documents that match a query in an index"
  [{:keys [uri cm]} :- ESConn
   index-names :- [s/Str]
   mapping :- (s/maybe s/Str)
   q :- ESQuery
   wait-for-completion? :- s/Bool
   refresh? :- Refresh]
  (safe-es-read
   (client/post
    (delete-by-query-uri uri index-names mapping)
    (merge default-opts
           {:query-params {:refresh refresh?
                           :wait_for_completion wait-for-completion?}
            :form-params {:query q}
            :connection-manager cm}))))

(defn sort-params
  [sort_by sort_order]
  (let [sort-fields
        (map (fn [field]
               (let [[field-name field-order] (clojure.string/split field #":")]
                 {field-name
                  {:order (keyword (or field-order sort_order))}}))
             (clojure.string/split (name sort_by) #","))]

    {:sort (into {} sort-fields)}))

(defn params->pagination
  [{:keys [sort_by sort_order offset limit search_after]
    :or {sort_order :asc
         limit pagination/default-limit}}]
  (merge
   {}
   (when sort_by
     (sort-params sort_by sort_order))
   (when limit
     {:size limit})
   (when (and offset
              (not search_after))
     {:from offset})
   (when search_after
     {:from 0
      :search_after search_after})))

(defn generate-es-params
  [query aggs params]
  (cond-> (into (params->pagination params)
                (select-keys params [:sort :_source]))
    query (assoc :query query)
    aggs (assoc :aggs aggs)))

(s/defn count-docs
  "Count documents on ES matching given query."
  ([{:keys [uri cm]} :- ESConn
   index-name :- s/Str
   mapping :- (s/maybe s/Str)
   query :- (s/maybe ESQuery)]
   (-> (client/post
        (count-uri uri index-name mapping)
        (merge default-opts
               (when query
                 {:form-params {:query query}})
               {:connection-manager cm}))
       safe-es-read
       :count))
  ([es-conn :- ESConn
    index-name :- s/Str
    mapping :- (s/maybe s/Str)]
   (count-docs es-conn index-name mapping nil)))

(defn- result-data
  [res full-hits?]
  (cond->> (-> res :hits :hits)
    (not full-hits?) (map :_source)))

(defn- pagination-params
  [{:keys [_scroll_id hits]}
   {:keys [from size search_after]}]
  {:offset from
   :limit size
   :sort (-> hits :hits last :sort)
   :search_after search_after
   :hits (:total hits 0)})

(defn- format-result
  [{:keys [aggregations] :as res}
   es-params
   full-hits?]
  (cond-> (pagination/response (result-data res full-hits?)
                               (pagination-params res es-params))
    aggregations (assoc :aggs aggregations)))

(s/defn query
  "Search for documents on ES using any query. Performs aggregations when specified."
  ([{:keys [uri cm]} :- ESConn
    index-name :- (s/maybe s/Str)
    mapping :- (s/maybe s/Str)
    q :- (s/maybe ESQuery)
    aggs :- (s/maybe ESAggs)
    {:keys [full-hits? scroll]
     :as params} :- s/Any]
   (let [es-params (generate-es-params q aggs params)
         res (safe-es-read
              (client/post
               (search-uri uri index-name mapping)
               (merge default-opts
                      {:form-params es-params
                       :connection-manager cm}
                      (when scroll
                        {:query-params {:scroll scroll}}))))]
     (log/debug "search-docs:" es-params)
     (format-result res es-params full-hits?)))
  ([es-conn index-name mapping q params]
   (query es-conn index-name mapping q nil params)))

(s/defn search-docs
  "Search for documents on ES using a query string search.  Also applies a filter map, converting
   the values in the all-of into must match terms."
  [es-conn :- ESConn
   index-name :- (s/maybe s/Str)
   mapping :- (s/maybe s/Str)
   es-query :- (s/maybe ESQuery)
   all-of :- (s/maybe {s/Any s/Any})
   params :- s/Any]
  (let [bool-query (q/filter-map->terms-query all-of es-query)]
    (query es-conn index-name mapping bool-query params)))
