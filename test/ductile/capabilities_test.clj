(ns ductile.capabilities-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [ductile.capabilities :as sut]
            [ductile.conn]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :once validate-schemas)

(deftest parse-version-test
  (testing "parse-version parses major.minor.patch correctly"
    (is (= {:major 7 :minor 17 :patch 0}
           (sut/parse-version "7.17.0")))
    (is (= {:major 2 :minor 19 :patch 0}
           (sut/parse-version "2.19.0")))
    (is (= {:major 3 :minor 1 :patch 0}
           (sut/parse-version "3.1.0"))))

  (testing "parse-version handles versions without patch"
    (is (= {:major 7 :minor 17}
           (sut/parse-version "7.17"))))

  (testing "parse-version returns nil for nil input"
    (is (nil? (sut/parse-version nil)))))

(deftest detect-engine-test
  (testing "detect-engine identifies Elasticsearch from cluster info"
    (let [es-cluster-info {:name "node-1"
                           :cluster_name "elasticsearch"
                           :version {:number "7.17.0"
                                     :build_flavor "default"
                                     :build_type "docker"}
                           :tagline "You Know, for Search"}
          result (sut/detect-engine es-cluster-info)]
      (is (= :elasticsearch (:engine result)))
      (is (= {:major 7 :minor 17 :patch 0} (:version result)))
      (is (= "default" (:build-flavor result)))))

  (testing "detect-engine identifies OpenSearch from cluster info"
    (let [os-cluster-info {:name "node-1"
                           :cluster_name "opensearch"
                           :version {:distribution "opensearch"
                                     :number "2.19.0"
                                     :build_type "docker"}
                           :tagline "The OpenSearch Project: https://opensearch.org/"}
          result (sut/detect-engine os-cluster-info)]
      (is (= :opensearch (:engine result)))
      (is (= {:major 2 :minor 19 :patch 0} (:version result)))
      (is (= "opensearch" (:distribution result)))))

  (testing "detect-engine handles OpenSearch 3.x"
    (let [os3-cluster-info {:name "node-1"
                            :cluster_name "opensearch"
                            :version {:distribution "opensearch"
                                      :number "3.1.0"
                                      :build_type "docker"}
                            :tagline "The OpenSearch Project: https://opensearch.org/"}
          result (sut/detect-engine os3-cluster-info)]
      (is (= :opensearch (:engine result)))
      (is (= {:major 3 :minor 1 :patch 0} (:version result))))))

(deftest version-compare-test
  (testing "version-compare compares versions correctly"
    (is (< (sut/version-compare {:major 7 :minor 10 :patch 0}
                                 {:major 7 :minor 17 :patch 0})
           0)
        "7.10.0 < 7.17.0")

    (is (= (sut/version-compare {:major 7 :minor 17 :patch 0}
                                 {:major 7 :minor 17 :patch 0})
           0)
        "7.17.0 == 7.17.0")

    (is (> (sut/version-compare {:major 8 :minor 0 :patch 0}
                                 {:major 7 :minor 17 :patch 0})
           0)
        "8.0.0 > 7.17.0")

    (is (< (sut/version-compare {:major 2 :minor 19 :patch 0}
                                 {:major 3 :minor 1 :patch 0})
           0)
        "2.19.0 < 3.1.0")

    (is (> (sut/version-compare {:major 7 :minor 17 :patch 1}
                                 {:major 7 :minor 17 :patch 0})
           0)
        "7.17.1 > 7.17.0")))

(deftest version-gte-test
  (testing "version>=? returns true when v1 >= v2"
    (is (sut/version>=? {:major 7 :minor 17}
                        {:major 7 :minor 10})
        "7.17 >= 7.10")

    (is (sut/version>=? {:major 7 :minor 17}
                        {:major 7 :minor 17})
        "7.17 >= 7.17")

    (is (sut/version>=? {:major 8 :minor 0}
                        {:major 7 :minor 17})
        "8.0 >= 7.17"))

  (testing "version>=? returns false when v1 < v2"
    (is (not (sut/version>=? {:major 7 :minor 10}
                             {:major 7 :minor 17}))
        "7.10 < 7.17")))

(deftest version-lt-test
  (testing "version<? returns true when v1 < v2"
    (is (sut/version<? {:major 7 :minor 10}
                       {:major 7 :minor 17})
        "7.10 < 7.17")

    (is (sut/version<? {:major 2 :minor 19}
                       {:major 3 :minor 1})
        "2.19 < 3.1"))

  (testing "version<? returns false when v1 >= v2"
    (is (not (sut/version<? {:major 7 :minor 17}
                            {:major 7 :minor 17}))
        "7.17 not < 7.17")

    (is (not (sut/version<? {:major 8 :minor 0}
                            {:major 7 :minor 17}))
        "8.0 not < 7.17")))

(deftest verify-connection-test
  (testing "verify-connection integrates cluster info fetching and engine detection"
    ;; This test uses a mock request-fn to simulate the connection
    (let [mock-es-response {:name "test-node"
                            :version {:number "7.17.0"
                                      :build_flavor "default"}
                            :tagline "You Know, for Search"}
          mock-request-fn (fn [_req]
                            {:status 200
                             :body mock-es-response})
          conn (ductile.conn/connect
                {:host "localhost"
                 :port 9200
                 :version 7
                 :engine :elasticsearch
                 :request-fn mock-request-fn})
          result (sut/verify-connection conn)]
      (is (= :elasticsearch (:engine result)))
      (is (= {:major 7 :minor 17 :patch 0} (:version result)))))

  (testing "verify-connection detects OpenSearch"
    (let [mock-os-response {:name "test-node"
                            :version {:distribution "opensearch"
                                      :number "2.19.0"}
                            :tagline "The OpenSearch Project: https://opensearch.org/"}
          mock-request-fn (fn [_req]
                            {:status 200
                             :body mock-os-response})
          conn (ductile.conn/connect
                {:host "localhost"
                 :port 9200
                 :version 2
                 :engine :opensearch
                 :request-fn mock-request-fn})
          result (sut/verify-connection conn)]
      (is (= :opensearch (:engine result)))
      (is (= {:major 2 :minor 19 :patch 0} (:version result)))
      (is (= "opensearch" (:distribution result))))))
