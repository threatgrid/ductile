(ns ductile.conn-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [ductile.conn :as sut]
            [schema.test :refer [validate-schemas]]))

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
                :throw-exceptions false}]

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
    (is (= (assoc common
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
