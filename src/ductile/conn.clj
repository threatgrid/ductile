(ns ductile.conn
  (:require [clj-http.client :as client]
            [clj-http.conn-mgr :refer [make-reusable-conn-manager shutdown-manager]]
            [clojure.tools.logging :as log]
            [ductile.auth :as auth]
            [ductile.schemas :refer [ConnectParams ESConn]]
            [schema.core :as s]))

(def default-connection-ttl 60)
(def default-validate-after-inactivity 5000)
(def default-threads 100)
(def default-per-route 100)
(def default-connection-timeout 10000)

(def default-opts
  {:as :json
   :content-type :json
   :throw-exceptions false})

(s/defn make-http-opts :- (s/pred map?)
  ([{:keys [cm auth timeouts]} :- (s/maybe ESConn)
    opts :- (s/pred map?)
    query-params-keys :- (s/maybe (s/pred coll?))
    form-params :- (s/maybe (s/pred map?))
    body :- s/Any]
   (cond-> default-opts
     auth (into auth)
     body (assoc :body body)
     form-params (assoc :form-params form-params)
     cm (assoc :connection-manager cm)
     (:connection-timeout timeouts) (assoc :connection-timeout (:connection-timeout timeouts))
     (:socket-timeout timeouts) (assoc :socket-timeout (:socket-timeout timeouts))
     (seq query-params-keys) (assoc :query-params
                                    (select-keys opts query-params-keys))))
  ([conn opts query-params-keys] (make-http-opts conn opts query-params-keys nil nil))
  ([conn opts] (make-http-opts conn opts [] nil nil))
  ([conn] (make-http-opts conn {} [] nil nil)))

(defn make-connection-manager
  "Create a reusable connection manager with the given options.

   Options:
   - :connection-ttl - how long connections live in the pool in seconds
   - :threads - max total connections
   - :default-per-route - max connections per route
   - :insecure? - allow insecure SSL (self-signed certs)
   - :validate-after-inactivity - check idle connections before reuse (in ms)
     Helps prevent NoHttpResponseException from stale connections."
  [{:keys [connection-ttl validate-after-inactivity] :as opts}]
  (let [;; clj-http uses :timeout for TTL
        cm-opts (-> opts
                    (dissoc :connection-ttl :validate-after-inactivity)
                    (assoc :timeout connection-ttl))
        conn-mgr (make-reusable-conn-manager cm-opts)]
    (when (some? validate-after-inactivity)
      (.setValidateAfterInactivity conn-mgr (int validate-after-inactivity)))
    conn-mgr))

(s/defn connect :- ESConn
  "Instantiate an ES conn from ConnectParams props.

  To intercept all ES HTTP requests, set :request-fn
  to function with the same interface as the 1-argument
  arity of `clj-http.client/request`.

  Connection pool options:
  - :connection-ttl - how long connections live in the pool in seconds (default: 60)
  - :validate-after-inactivity - check idle connections before reuse, in ms (default: 5000)
    Prevents NoHttpResponseException from stale connections closed server-side.
  - :threads - max total connections in pool (default: 100)
  - :default-per-route - max connections per route (default: 100)
  - :insecure? - allow insecure SSL connections, e.g. self-signed certs (default: false)

  Request timeout options (applied to every request):
  - :connection-timeout - time to establish TCP connection in ms (default: 10000)
  - :socket-timeout - time to wait for data in ms (default: none, for long-running operations)"
  [{:keys [protocol host port connection-ttl version engine auth request-fn
           validate-after-inactivity threads default-per-route insecure?
           connection-timeout socket-timeout]
    :or {protocol :http
         request-fn client/request
         connection-ttl default-connection-ttl
         validate-after-inactivity default-validate-after-inactivity
         threads default-threads
         default-per-route default-per-route
         insecure? false
         connection-timeout default-connection-timeout
         version 7
         engine :elasticsearch}} :- ConnectParams]
  (let [timeouts (cond-> {}
                   connection-timeout (assoc :connection-timeout connection-timeout)
                   socket-timeout (assoc :socket-timeout socket-timeout))
        conn {:cm (make-connection-manager
                   {:connection-ttl connection-ttl
                    :validate-after-inactivity validate-after-inactivity
                    :threads threads
                    :default-per-route default-per-route
                    :insecure? insecure?})
              :request-fn request-fn
              :uri (format "%s://%s:%s" (name protocol) host port)
              :version version
              :engine engine}]
    (cond-> conn
      auth (assoc :auth (auth/http-options auth))
      (seq timeouts) (assoc :timeouts timeouts))))

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
