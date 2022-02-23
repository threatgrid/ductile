(ns ductile.conn
  (:require [clj-http.client :as client]
            [clj-http.conn-mgr :refer [make-reusable-conn-manager shutdown-manager]]
            [clojure.tools.logging :as log]
            [ductile.auth :as auth]
            [ductile.schemas :refer [ConnectParams ESConn]]
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

(s/defn make-http-opts :- (s/pred map?)
  ([{:keys [cm auth]} :- (s/maybe ESConn)
    opts :- (s/pred map?)
    query-params-keys :- (s/maybe (s/pred coll?))
    form-params :- (s/maybe (s/pred map?))
    body :- s/Any]
   (cond-> default-opts
     auth (into auth)
     body (assoc :body body)
     form-params (assoc :form-params form-params)
     cm (assoc :connection-manager cm)
     (seq query-params-keys) (assoc :query-params
                                    (select-keys opts query-params-keys))))
  ([conn opts query-params-keys] (make-http-opts conn opts query-params-keys nil nil))
  ([conn opts] (make-http-opts conn opts [] nil nil)))

(defn make-connection-manager [cm-options]
  (make-reusable-conn-manager cm-options))

(s/defn connect :- ESConn
  "Instantiate an ES conn from ConnectParams props.
  
  To intercept all ES HTTP requests, set :request-fn
  to function with the same interface as the 1-argument
  arity of `clj-http.client/request`."
  [{:keys [protocol host port timeout version auth request-fn]
    :or {protocol :http
         request-fn client/request
         timeout default-timeout
         version 7}} :- ConnectParams]
  (let [conn {:cm (make-connection-manager
                   (cm-options {:timeout timeout}))
              :request-fn request-fn
              :uri (format "%s://%s:%s" (name protocol) host port)
              :version version}]
    (cond-> conn
      auth (assoc :auth (auth/http-options auth)))))

(s/defn close [conn :- ESConn]
  (-> conn :cm shutdown-manager))

(defn safe-es-read [{:keys [status body]
                     :as res}]
  (case (int status)
    200 body
    201 body
    404 nil
    401 (do (log/warn "Unauthorized ES Request:" res)
            (throw (ex-info "Unauthorized ES Request"
                            {:type ::unauthorized
                             :es-http-res res})))
    400 (do (log/warn "ES Invalid Request:" res)
            (throw (ex-info "ES query parsing error"
                            {:type ::invalid-request
                             :es-http-res res})))
    (do (log/warn "ES Unknown Error:" res)
        (throw (ex-info "ES Unknown Error"
                        {:type ::es-unknown-error
                         :es-http-res res})))))

(defn safe-es-bulk-read [body]
  (if (:errors body)
    (throw (ex-info "ES bulk operation failed"
                    {:type ::es-unknown-error
                     :es-http-res-body body}))
    body))
