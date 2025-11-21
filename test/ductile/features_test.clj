(ns ductile.features-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [ductile.conn :as es-conn]
            [ductile.features :as sut]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :once validate-schemas)

;; Note: These connections use proper ESConn schema with integer versions
;; For testing purposes, we create minimal valid connections
(defn make-test-conn [engine version]
  (es-conn/connect
   {:host "localhost"
    :port 9200
    :version version
    :engine engine
    :request-fn (fn [_] {:status 200 :body {}})}))

(def es7-conn (make-test-conn :elasticsearch 7))
(def es6-conn (make-test-conn :elasticsearch 6))
(def es5-conn (make-test-conn :elasticsearch 5))
(def os2-conn (make-test-conn :opensearch 2))
(def os3-conn (make-test-conn :opensearch 3))

(deftest supports-ilm-test
  (testing "ILM is supported in Elasticsearch 7+"
    (is (sut/supports-ilm? es7-conn)
        "ES 7.x supports ILM"))

  (testing "ILM is not supported in Elasticsearch < 7"
    (is (not (sut/supports-ilm? es6-conn))
        "ES 6.x does not support ILM")
    (is (not (sut/supports-ilm? es5-conn))
        "ES 5.x does not support ILM"))

  (testing "ILM is not supported in OpenSearch"
    (is (not (sut/supports-ilm? os2-conn))
        "OpenSearch 2.x does not support ILM")
    (is (not (sut/supports-ilm? os3-conn))
        "OpenSearch 3.x does not support ILM")))

(deftest supports-ism-test
  (testing "ISM is supported in all OpenSearch versions"
    (is (sut/supports-ism? os2-conn)
        "OpenSearch 2.x supports ISM")
    (is (sut/supports-ism? os3-conn)
        "OpenSearch 3.x supports ISM"))

  (testing "ISM is not supported in Elasticsearch"
    (is (not (sut/supports-ism? es5-conn))
        "ES 5.x does not support ISM")
    (is (not (sut/supports-ism? es6-conn))
        "ES 6.x does not support ISM")
    (is (not (sut/supports-ism? es7-conn))
        "ES 7.x does not support ISM")))

(deftest supports-data-streams-test
  (testing "Data streams in Elasticsearch 7.x (conservative: all 7.x)"
    (is (sut/supports-data-streams? es7-conn)
        "ES 7.x supports data streams"))

  (testing "Data streams not in Elasticsearch < 7"
    (is (not (sut/supports-data-streams? es6-conn))
        "ES 6.x does not support data streams")
    (is (not (sut/supports-data-streams? es5-conn))
        "ES 5.x does not support data streams"))

  (testing "Data streams in OpenSearch 2.0+"
    (is (sut/supports-data-streams? os2-conn)
        "OpenSearch 2.x supports data streams")
    (is (sut/supports-data-streams? os3-conn)
        "OpenSearch 3.x supports data streams")))

(deftest supports-composable-templates-test
  (testing "Composable templates in Elasticsearch 7.x (conservative: all 7.x)"
    (is (sut/supports-composable-templates? es7-conn)
        "ES 7.x supports composable templates"))

  (testing "Composable templates not in Elasticsearch < 7"
    (is (not (sut/supports-composable-templates? es6-conn))
        "ES 6.x does not support composable templates")
    (is (not (sut/supports-composable-templates? es5-conn))
        "ES 5.x does not support composable templates"))

  (testing "Composable templates in all OpenSearch versions"
    (is (sut/supports-composable-templates? os2-conn)
        "OpenSearch 2.x supports composable templates")
    (is (sut/supports-composable-templates? os3-conn)
        "OpenSearch 3.x supports composable templates")))

(deftest supports-legacy-templates-test
  (testing "Legacy templates supported in all versions"
    (is (sut/supports-legacy-templates? es5-conn)
        "ES 5.x supports legacy templates")
    (is (sut/supports-legacy-templates? es6-conn)
        "ES 6.x supports legacy templates")
    (is (sut/supports-legacy-templates? es7-conn)
        "ES 7.x supports legacy templates")
    (is (sut/supports-legacy-templates? os2-conn)
        "OpenSearch 2.x supports legacy templates")
    (is (sut/supports-legacy-templates? os3-conn)
        "OpenSearch 3.x supports legacy templates")))

(deftest supports-doc-types-test
  (testing "Document types required in Elasticsearch < 7"
    (is (sut/supports-doc-types? es5-conn)
        "ES 5.x requires doc types")
    (is (sut/supports-doc-types? es6-conn)
        "ES 6.x requires doc types"))

  (testing "Document types removed in Elasticsearch 7+"
    (is (not (sut/supports-doc-types? es7-conn))
        "ES 7.x does not use doc types"))

  (testing "Document types not used in OpenSearch"
    (is (not (sut/supports-doc-types? os2-conn))
        "OpenSearch 2.x does not use doc types")
    (is (not (sut/supports-doc-types? os3-conn))
        "OpenSearch 3.x does not use doc types")))

(deftest lifecycle-management-type-test
  (testing "Lifecycle management type detection"
    (is (= :ilm (sut/lifecycle-management-type es7-conn))
        "ES 7.x uses ILM")

    (is (nil? (sut/lifecycle-management-type es6-conn))
        "ES 6.x has no lifecycle management")

    (is (nil? (sut/lifecycle-management-type es5-conn))
        "ES 5.x has no lifecycle management")

    (is (= :ism (sut/lifecycle-management-type os2-conn))
        "OpenSearch 2.x uses ISM")

    (is (= :ism (sut/lifecycle-management-type os3-conn))
        "OpenSearch 3.x uses ISM")))

(deftest get-feature-summary-test
  (testing "Feature summary for Elasticsearch 7.x"
    (let [summary (sut/get-feature-summary es7-conn)]
      (is (= true (:ilm summary)))
      (is (= false (:ism summary)))
      (is (= true (:data-streams summary)))
      (is (= true (:composable-templates summary)))
      (is (= true (:legacy-templates summary)))
      (is (= false (:doc-types summary)))))

  (testing "Feature summary for Elasticsearch 5.x"
    (let [summary (sut/get-feature-summary es5-conn)]
      (is (= false (:ilm summary)))
      (is (= false (:ism summary)))
      (is (= false (:data-streams summary)))
      (is (= false (:composable-templates summary)))
      (is (= true (:legacy-templates summary)))
      (is (= true (:doc-types summary)))))

  (testing "Feature summary for OpenSearch 2.x"
    (let [summary (sut/get-feature-summary os2-conn)]
      (is (= false (:ilm summary)))
      (is (= true (:ism summary)))
      (is (= true (:data-streams summary)))
      (is (= true (:composable-templates summary)))
      (is (= true (:legacy-templates summary)))
      (is (= false (:doc-types summary))))))

(deftest require-feature-test
  (testing "require-feature! succeeds when feature is supported"
    (is (nil? (sut/require-feature! es7-conn :ilm "ILM required"))
        "Should not throw when feature is supported"))

  (testing "require-feature! throws when feature is not supported"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"ILM required"
         (sut/require-feature! os2-conn :ilm "ILM required"))
        "Should throw when ILM not supported in OpenSearch")

    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Data streams required"
         (sut/require-feature! es6-conn :data-streams "Data streams required"))
        "Should throw when data streams not supported in ES6")))
