(ns ductile.auth.api-key-test
  (:require [ductile.auth.api-key :as sut]
            [ductile.test-helpers :refer [connect basic-auth-opts]]
            [clojure.test :refer [deftest is]]
            [clojure.set :as set]))

(deftest api-key-uri-test
  (is (= "http://localhost:9200/_security/api_key"
         (sut/api-key-uri "http://localhost:9200")))
  (is (= "https://cisco.com/_security/api_key"
         (sut/api-key-uri "https://cisco.com"))))

(deftest create-api-key!-test
  (is (set/subset? #{:id :api_key}
                   (-> (connect 7 basic-auth-opts)
                       (sut/create-api-key! {:name "my-api-key"})
                       keys
                       set))))
