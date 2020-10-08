(ns ductile.conn
  (:require [clj-http.conn-mgr :refer [make-reusable-conn-manager
                                       shutdown-manager]]
            [clojure.tools.logging :as log]
            [ductile.schemas :refer [ConnectParams ESConn]]
            [medley.core :refer [assoc-some]]
            [schema.core :as s]))

(def default-timeout 30000)

(defn cm-options [{:keys [timeout]}]
  {:timeout timeout
   :threads 100
   :default-per-route 100})

(def default-opts
  {:as :json
   :content-type :json
   :throw-exceptions false})

(defn make-http-opts
  ([cm
    opts
    query-params-keys
    form-params
    body]
   (cond-> default-opts
     body (assoc :body body)
     form-params (assoc :form-params form-params)
     cm (assoc :connection-manager cm)
     (seq query-params-keys) (assoc :query-params
                                     (select-keys opts query-params-keys))))
  ([cm opts query-params-keys] (make-http-opts cm opts query-params-keys nil nil))
  ([cm] (make-http-opts cm {} [] nil nil)))

(defn make-connection-manager [cm-options]
  (make-reusable-conn-manager cm-options))

(s/defn connect :- ESConn
  "instantiate an ES conn from props"
  [{:keys [protocol host port timeout version]
    :or {protocol :http
         timeout default-timeout
         version 7}} :- ConnectParams]
  (assoc-some
   {:cm (make-connection-manager
         (cm-options {:timeout timeout}))
    :uri (format "%s://%s:%s" (name protocol) host port)}
    :version version))

(s/defn close [conn :- ESConn]
  (-> conn :cm shutdown-manager))

(defn safe-es-read [{:keys [status body]
                     :as res}]
  (case (int status)
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
