(ns ductile.conn
  (:require [clj-http.conn-mgr :refer [make-reusable-conn-manager]]
            [clojure.tools.logging :as log]
            [ductile.schemas :refer [ESConn ConnectParams]]
            [schema.core :as s])
  (:import [org.apache.http.impl.conn PoolingClientConnectionManager
            PoolingHttpClientConnectionManager]))

(def default-timeout 30000)

(defn cm-options [{:keys [timeout]}]
  {:timeout timeout
   :threads 100
   :default-per-route 100})

(def default-opts
  {:as :json
   :content-type :json
   :throw-exceptions false
   :query-params {}})

(defn make-default-opts
  [{:keys [_source]}]
  (if _source
    (update default-opts
            :query-params
            #(assoc % :_source (clojure.string/join "," (map name  _source))))
    default-opts))

(defn make-connection-manager [cm-options]
  (make-reusable-conn-manager cm-options))

(s/defn connect :- ESConn
  "instantiate an ES conn from props"
  [{:keys [transport host port timeout]
    :or {transport :http
         timeout default-timeout}} :- ConnectParams]

  {:cm (make-connection-manager
        (cm-options {:timeout timeout}))
   :uri (format "%s://%s:%s" (name transport) host port)})

(defn safe-es-read [{:keys [status body]
                     :as res}]
  (case status
    200 body
    201 body
    404 nil
    400 (do (log/warn "ES query parsing error:" res)
            (throw (ex-info "ES query parsing error"
                            {:type ::es-query-parsing-error
                             :es-http-res res})))
    (do (log/warn "ES query failed:" res)
        (throw (ex-info "ES query failed"
                        {:type ::es-unknown-error
                         :es-http-res res})))))

(defn safe-es-bulk-read [body]
  (if (:errors body)
    (throw (ex-info "ES bulk operation failed"
                    {:type ::es-unknown-error
                     :es-http-res-body body}))
    body))
