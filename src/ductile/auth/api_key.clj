(ns ductile.auth.api-key
  (:require [ductile.conn :refer [make-http-opts safe-es-read]]
           [ductile.schemas :refer [ESConn]]
           [schema.core :as s]))

(s/defn api-key-uri
  [uri :- s/Str]
  (str uri "/_security/api_key"))

(s/defn create-api-key!
  [{:keys [uri request-fn] :as conn} :- ESConn
   key-desc :- (s/pred map?)]
  (-> (make-http-opts conn {})
      (assoc :method :post
             :form-params key-desc
             :url (api-key-uri uri))
      request-fn
      safe-es-read))
