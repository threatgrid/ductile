(ns ductile.uri
  (:require [clojure.string :as string])
  (:import (java.net URLEncoder URI)))

(defn uri-encode [^String st]
  (when st
    (not-empty (.replace (URLEncoder/encode st "UTF-8") "+" "%20"))))

(defn uri ^String [^String base-uri & path-segments]
  (-> (string/join "/" [base-uri (string/join "/" (keep identity path-segments))])
      (URI.)
      (.normalize)
      (.toString)))
