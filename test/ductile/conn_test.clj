(ns ductile.conn-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [ductile.conn :as sut]
            [ductile.capabilities :as caps]
            [ductile.test-helpers :as helpers]
            [schema.test :refer [validate-schemas]])
  (:import [org.apache.http.impl.conn PoolingHttpClientConnectionManager]))

(use-fixtures :once validate-schemas)

(deftest connect-test
  (is (= "https://cisco.com:9201"
         (:uri (sut/connect {:protocol :https
                             :host "cisco.com"
                             :port 9201}))))
  (is (= "http://127.0.0.1:9200"
         (:uri (sut/connect {:host "127.0.0.1"
                             :port 9200})))))

(deftest make-http-opts-test
  (let [{:keys [cm] :as conn-wo-auth}
        (sut/connect {:host "127.0.0.1"
                      :port 9200})
        doc {:foo :bar
             :deeper {:for :bar}}
        opts {:refresh "true"
              :_source ["foo" "bar"]
              :param-1 "param-1"
              :param-2 "param-2"}
        common {:as :json
                :content-type :json
                :throw-exceptions false
                :connection-timeout 10000}]

    (is (= (assoc common
                  :connection-manager cm
                  :query-params (dissoc opts :param-2))
           (sut/make-http-opts conn-wo-auth
                               opts
                               [:refresh
                                :_source
                                :param-1])))
    (is (= (assoc common
                  :connection-manager cm
                  :query-params (dissoc opts :param-2)
                  :form-params doc)
            (sut/make-http-opts conn-wo-auth
                                opts
                                [:refresh
                                 :_source
                                 :param-1]
                                doc
                                nil)))
    (is (= (assoc common
                  :connection-manager cm
                  :query-params (dissoc opts :param-2)
                  :body doc)
           (sut/make-http-opts conn-wo-auth
                               opts
                               [:refresh
                                :_source
                                :param-1]
                               nil
                               doc)))
    (is (= (assoc common
                  :connection-manager cm
                  :query-params {:param-1 "param-1"})
           (sut/make-http-opts conn-wo-auth opts [:param-1])))
    (is (= (assoc common
                  :connection-manager cm)
           (sut/make-http-opts conn-wo-auth opts [])
           (sut/make-http-opts conn-wo-auth opts nil)))
    (is (= (assoc (dissoc common :connection-timeout)
                  :query-params {:param-1 "param-1"})
           (sut/make-http-opts nil opts [:param-1])))))

(deftest testing-auth-params
  (testing "auth options should be properly prepared and integrated in http params."
    (let [test-auth (fn [auth-params expected]
                      (let [{:keys [auth] :as conn}
                            (sut/connect {:host "127.0.0.1"
                                          :port 9200
                                          :auth auth-params})
                            auth-http-opts (-> (sut/make-http-opts conn {})
                                               (select-keys (keys auth)))]
                        (is (= expected auth)
                            "connect shall properly format auth params")
                        (is (= auth auth-http-opts)
                            "make-http-opts shall properly consider http options")))]
      (doseq [[auth-params expected]
              [[{:type :headers
                 :params {:authorization "ApiKey bmdrdkxuWUI0WmVoR1cxcVUtWHo6NkhNbkFDUFJTVldTTXZaQ2Y5VmNHZw=="}}
                {:headers
                 {:authorization "ApiKey bmdrdkxuWUI0WmVoR1cxcVUtWHo6NkhNbkFDUFJTVldTTXZaQ2Y5VmNHZw=="}}]
               [{:type :basic-auth
                 :params {:user "the-login" :pwd "the-pwd"}}
                {:basic-auth
                 ["the-login" "the-pwd"]}]
               [{:type :oauth-token
                 :params {:token "any-token"}}
                {:oauth-token "any-token"}]]]
               (test-auth auth-params expected)))))

(deftest engine-test
  (testing "engine parameter defaults to :elasticsearch for backward compatibility"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200})]
      (is (= :elasticsearch (:engine conn))
          "Default engine should be :elasticsearch")))

  (testing "engine parameter can be explicitly set to :elasticsearch"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :engine :elasticsearch})]
      (is (= :elasticsearch (:engine conn))
          "Explicitly set :elasticsearch engine")))

  (testing "engine parameter can be set to :opensearch"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :engine :opensearch})]
      (is (= :opensearch (:engine conn))
          "OpenSearch engine should be set correctly")))

  (testing "version defaults to 7 for elasticsearch"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :engine :elasticsearch})]
      (is (= 7 (:version conn))
          "Default version should be 7")))

  (testing "version can be set for opensearch"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :engine :opensearch
                             :version 2})]
      (is (= 2 (:version conn))
          "OpenSearch version 2 should be set correctly")
      (is (= :opensearch (:engine conn))
          "Engine should be :opensearch"))))

(deftest validate-after-inactivity-test
  (testing "validate-after-inactivity defaults to 5000ms"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200})
          ^PoolingHttpClientConnectionManager cm (:cm conn)]
      (is (= 5000 (.getValidateAfterInactivity cm))
          "Default validate-after-inactivity should be 5000ms")))

  (testing "validate-after-inactivity can be customized"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :validate-after-inactivity 10000})
          ^PoolingHttpClientConnectionManager cm (:cm conn)]
      (is (= 10000 (.getValidateAfterInactivity cm))
          "Custom validate-after-inactivity should be set")))

  (testing "validate-after-inactivity can be disabled with 0"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :validate-after-inactivity 0})
          ^PoolingHttpClientConnectionManager cm (:cm conn)]
      ;; When 0 is passed, validation is effectively disabled
      (is (= 0 (.getValidateAfterInactivity cm))
          "Zero validate-after-inactivity disables validation"))))

(deftest connection-pool-options-test
  (testing "pool size defaults"
    (let [conn (sut/connect {:host "127.0.0.1" :port 9200})
          ^PoolingHttpClientConnectionManager cm (:cm conn)]
      (is (= 100 (.getMaxTotal cm))
          "Default max total connections should be 100")
      (is (= 100 (.getDefaultMaxPerRoute cm))
          "Default max per route should be 100")))

  (testing "pool sizes can be customized"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :threads 50
                             :default-per-route 25})
          ^PoolingHttpClientConnectionManager cm (:cm conn)]
      (is (= 50 (.getMaxTotal cm))
          "Custom max total connections")
      (is (= 25 (.getDefaultMaxPerRoute cm))
          "Custom max per route")))

  (testing "insecure? option is accepted"
    ;; Just verify it doesn't throw - actual SSL behavior tested in integration
    (is (some? (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :insecure? true}))
        "Connection with insecure? should succeed")))

(deftest request-timeout-options-test
  (testing "connection-timeout defaults to 10000ms"
    (let [conn (sut/connect {:host "127.0.0.1" :port 9200})]
      (is (= 10000 (get-in conn [:timeouts :connection-timeout]))
          "Default connection-timeout should be 10000ms")
      (is (nil? (get-in conn [:timeouts :socket-timeout]))
          "socket-timeout should not be set by default")))

  (testing "connection-timeout is stored in conn"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :connection-timeout 5000})]
      (is (= 5000 (get-in conn [:timeouts :connection-timeout]))
          "connection-timeout should be stored")))

  (testing "socket-timeout is stored in conn"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :socket-timeout 10000})]
      (is (= 10000 (get-in conn [:timeouts :socket-timeout]))
          "socket-timeout should be stored")))

  (testing "both timeouts can be set together"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :connection-timeout 5000
                             :socket-timeout 10000})]
      (is (= {:connection-timeout 5000 :socket-timeout 10000}
             (:timeouts conn))
          "Both timeouts should be stored")))

  (testing "timeouts are applied to http options"
    (let [conn (sut/connect {:host "127.0.0.1"
                             :port 9200
                             :connection-timeout 5000
                             :socket-timeout 10000})
          http-opts (sut/make-http-opts conn {})]
      (is (= 5000 (:connection-timeout http-opts))
          "connection-timeout should be in http opts")
      (is (= 10000 (:socket-timeout http-opts))
          "socket-timeout should be in http opts"))))

(defn- connect-with-validation
  "Connect to test ES/OpenSearch with specified validate-after-inactivity setting"
  [engine version validate-after-inactivity]
  (sut/connect
   (cond-> {:host "localhost"
            :port (helpers/engine-port engine version)
            :version version
            :engine engine
            :validate-after-inactivity validate-after-inactivity}
     (= engine :elasticsearch) (assoc :auth helpers/basic-auth-opts))))

(deftest ^:integration validate-after-inactivity-behavior-test
  (testing "connection validation allows request to succeed after idle period"
    (helpers/for-each-es-version
      "requests succeed with validation enabled after idle"
      nil
      ;; Create a connection with validation enabled (short: 1ms)
      (let [conn-with-validation (connect-with-validation engine version 1)
            ^PoolingHttpClientConnectionManager cm (:cm conn-with-validation)]
        (try
          ;; Verify validation setting is applied
          (is (= 1 (.getValidateAfterInactivity cm))
              "Validation should be set to 1ms")

          ;; Make initial request to establish a pooled connection
          (is (map? (caps/get-cluster-info conn-with-validation))
              "Initial request should succeed")

          ;; Wait longer than validate-after-inactivity to trigger validation
          (Thread/sleep 10)

          ;; Make another request - validation will check the connection
          (is (map? (caps/get-cluster-info conn-with-validation))
              "Second request should succeed after validation")

          ;; Verify pool is working correctly
          (let [stats (.getTotalStats cm)]
            (is (>= (.getMax stats) 1)
                "Connection pool should be configured"))
          (finally
            (sut/close conn-with-validation))))))

  (testing "multiple requests work correctly with connection pooling and validation"
    (helpers/for-each-es-version
      "sequential requests with validation"
      nil
      (let [conn (connect-with-validation engine version 100)
            ^PoolingHttpClientConnectionManager cm (:cm conn)]
        (try
          ;; Make multiple requests with delays to exercise pool behavior
          (dotimes [_ 5]
            (is (map? (caps/get-cluster-info conn))
                "Each request should succeed")
            (Thread/sleep 50))

          ;; Verify no connection leaks
          (let [stats (.getTotalStats cm)]
            (is (<= (.getLeased stats) 0)
                "All connections should be returned to pool after requests"))
          (finally
            (sut/close conn)))))))
