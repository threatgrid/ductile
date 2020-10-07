(ns ductile.test-helpers
  (:require [clojure.test :refer [testing]]
            [ductile.conn :as es-conn]))

(defmacro for-each-es-version [msg clean & body]
  "for each version, init an ES connection, expose anaphora version and conn, and apply test body."
  `(doseq [~'version [5 7]]
     (let [~'conn (es-conn/connect {:host "localhost"
                                    :port (+ 9200 ~'version)
                                    :version ~'version})]
       (testing (format "%s (ES version: %s)" ~msg  ~'version))
       (if ~clean
         '(~clean))
       ~@body
       (if ~clean
         '(~clean)))))
