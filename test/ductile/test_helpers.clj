(ns ductile.test-helpers
  (:require [clojure.test :refer [testing]]
            [ductile.conn :as es-conn]))

(def basic-auth-opts
  {:type :basic-auth
   :params {:user "elastic" :pwd "ductile"}})

(defn connect
  [version auth-opts]
  (cond-> {:host "localhost"
           :port (+ 9200 version)
           :version version}
    (seq auth-opts) (assoc :auth auth-opts)
    :finally es-conn/connect))

(defmacro for-each-es-version [msg clean & body]
  "for each ES version:
- init an ES connection
- expose anaphoric `version` and `conn` to use in body
- wrap body with a `testing` block with with `msg` formatted with `version`
- call `clean` fn if not `nil` before and after body."
  {:style/indent 2}
  `(doseq [~'version [7]]
     (let [~'conn (connect ~'version basic-auth-opts)
           clean-fn# ~clean]
       (try
         (testing (format "%s (ES version: %s)" ~msg  ~'version)
           (when clean-fn#
             (clean-fn#))
           ~@body
           (when clean-fn#
             (clean-fn#)))
         (finally (es-conn/close ~'conn))))))
