(ns ductile.test-helpers
  (:require [clojure.test :refer [testing]]
            [ductile.conn :as es-conn]))

(defmacro for-each-es-version [msg clean & body]
  "for each ES version:
- init an ES connection
- expose anaphoras `version` and `conn` to use in body
- wrap body with a `testing` block with with `msg` formatted with `version`
- call `clean` fn if not `nil` before and after body."
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
