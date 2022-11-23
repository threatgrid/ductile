(ns ductile.auth
  (:require [ductile.schemas :refer [AuthParams  ApiKey BasicAuth OAuthToken]]
            [clojure.string :as string]
            [schema.core :as s])
  (:import (java.util Base64)))

(defn- encode-base64 [^String st]
  (.encodeToString (Base64/getEncoder) (.getBytes st "UTF-8")))

(s/defn api-key-auth
  "Generates Elasticsearch API keys authorization headers.
   The key is the base64 encoding of `id` and `api_key` joined by a colon.
   https://www.elastic.co/guide/en/kibana/7.10/api-keys.html"
  [{key-id :id
    :keys [api-key]} :- ApiKey]
  (let [api-key (encode-base64 (format "%s:%s" key-id api-key))]
    {:headers
     {:authorization
      (str "ApiKey " api-key)}}))

(s/defn basic-auth
  "Generates basic auth clj-http params."
  [{:keys [user pwd]} :- BasicAuth]
  {:basic-auth [user pwd]})

(s/defn oauth-token
  "Generate oauth clj-http params."
  [{:keys [token]} :- OAuthToken]
  {:oauth-token token})

(s/defn bearer
  "Generates oauth clj-http params for a bearer token."
  [{:keys [token]} :- OAuthToken]
  (let [bearer-token (if (string/starts-with? token "Bearer ")
                       token
                       (str "Bearer " token))]
    (oauth-token {:token bearer-token})))

(s/defn http-options
  "takes an AuthParams map and returns the proper clj-http request options"
  [{auth-type :type
    :keys [params]} :- AuthParams]
  (case auth-type
    :basic-auth (basic-auth params)
    :api-key (api-key-auth params)
    :oauth-token (oauth-token params)
    :bearer (bearer params)
    :headers {:headers params}))
