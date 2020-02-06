(ns ductile.conn-test
  (:require [ductile.conn :as sut]
            [clojure.test :refer [deftest is]]))

(deftest connect-test
  (is (= "https://cisco.com:9201"
         (:uri (sut/connect {:transport :https
                             :host "cisco.com"
                             :port 9201}))))
  (is (= "http://127.0.0.1:9200"
         (:uri (sut/connect {:host "127.0.0.1"
                             :port 9200})))))
