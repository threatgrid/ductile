(ns ductile.schemas
  "All ES related schemas should be defined here"
  (:require [schema.core :as s]
            [schema-tools.core :as st])
  (:import [org.apache.http.impl.conn PoolingClientConnectionManager
            PoolingHttpClientConnectionManager]))

(s/defschema ConnectParams
  (st/open-schema
   {:host s/Str
    :port s/Int
    (s/optional-key :protocol) (s/enum :http :https)
    (s/optional-key :version) s/Int
    (s/optional-key :timeout) s/Int}))

(s/defschema ESConn
  "an ES conn is a map with a
   connection manager and an index name"
  {:cm (s/either PoolingClientConnectionManager
                 PoolingHttpClientConnectionManager)
   :uri s/Str
   (s/optional-key :version) s/Int})

(s/defschema Refresh
  "ES refresh parameter, see
   https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-refresh.html"
  (s/enum "true" "false" "wait_for"))

(s/defschema CRUDOptions
  (st/open-schema
   (st/optional-keys
    {:refresh Refresh
     :retry_on_conflict s/Int
     :wait_for_completion s/Bool
     :_source (s/cond-pre s/Bool [s/Str])
     :mk-id (s/=> s/Any s/Any)
     :op_type (s/enum "create" "index")})))

(s/defschema ESConnState
  "a Store ESConnState shall contain an ESConn
   and all store properties"
  {:index s/Str
   :props {s/Any s/Any}
   :config {s/Any s/Any}
   :conn ESConn})


(s/defschema ESQuery {s/Keyword {s/Any s/Any}})
(s/defschema ESAgg {s/Keyword {s/Any s/Any}})
(s/defschema ESAggs {s/Keyword ESAgg})

(s/defschema BoolQueryParams
  "Bool query parameters"
  (st/optional-keys
   {:must [ESQuery]
    :filter [ESQuery]
    :should [ESQuery]
    :should-not [ESQuery]
    :minimum_should_match s/Int}))

(s/defschema BoolQuery
  "Bool query"
  {:bool BoolQueryParams})

(s/defschema IdsQuery
  "Ids query"
  {:ids {:values [s/Str]}})

(s/defschema RolloverConditions
  "Rollover conditions"
  (st/optional-keys
   {:max_age s/Str
    :max_docs s/Int
    :max_size s/Str}))
