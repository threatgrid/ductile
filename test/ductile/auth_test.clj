(ns ductile.auth-test
  (:require [ductile.auth :as sut]
            [ductile.auth.api-key :refer [create-api-key!]]
            [ductile.index :refer [get-template]]
            [ductile.test-helpers :refer [connect basic-auth-opts]]
            [clojure.test :refer [deftest testing is]])
  (:import [clojure.lang ExceptionInfo]))

(deftest api-key-auth-test
  (is (= {:headers
          {:authorization
           "ApiKey bmdrdkxuWUI0WmVoR1cxcVUtWHo6NkhNbkFDUFJTVldTTXZaQ2Y5VmNHZw=="}}
         (sut/api-key-auth {:id "ngkvLnYB4ZehGW1qU-Xz"
                            :api-key "6HMnACPRSVWSMvZCf9VcGg"}))))
(deftest basic-auth-test
  (is (= {:basic-auth ["the-login" "the-pwd"]}
         (sut/basic-auth {:user "the-login" :pwd "the-pwd"}))))

(deftest oauth-token-test
  (is (= {:oauth-token "any-token"}
         (sut/oauth-token {:token "any-token"}))))

(deftest bearer-test
  (is (= {:oauth-token "Bearer any-token"}
         (sut/bearer {:token "any-token"})))
  (is (= {:oauth-token "Bearer any-token"}
         (sut/bearer {:token "Bearer any-token"}))))

(deftest http-options-test
  (is (= {:headers
          {:authorization
           "ApiKey bmdrdkxuWUI0WmVoR1cxcVUtWHo6NkhNbkFDUFJTVldTTXZaQ2Y5VmNHZw=="}}
         (sut/http-options
          {:type :headers
           :params {:authorization "ApiKey bmdrdkxuWUI0WmVoR1cxcVUtWHo6NkhNbkFDUFJTVldTTXZaQ2Y5VmNHZw=="}})))
  (is (= {:headers
          {:authorization
           "ApiKey bmdrdkxuWUI0WmVoR1cxcVUtWHo6NkhNbkFDUFJTVldTTXZaQ2Y5VmNHZw=="}}
         (sut/http-options {:type :api-key
                            :params {:id "ngkvLnYB4ZehGW1qU-Xz"
                                     :api-key "6HMnACPRSVWSMvZCf9VcGg"}})))
  (is (= {:basic-auth ["the-login" "the-pwd"]}
         (sut/http-options {:type :basic-auth
                            :params {:user "the-login" :pwd "the-pwd"}})))
  (is (= {:oauth-token "any-token"}
         (sut/http-options {:type :oauth-token
                            :params {:token "any-token"}})))
  (is (= {:oauth-token "Bearer any-token"}
         (sut/http-options {:type :bearer
                            :params {:token "any-token"}}))))

(deftest ^:integration request-with-auth-test
  (testing "requesting Elasticsearch with valid and invalid basic auth"
    (let [;; authorized connections:
          conn-basic-auth (connect 7 basic-auth-opts)
          {key-id :id :keys [api_key]} (create-api-key! conn-basic-auth {:name "api-key-int-test"})
          conn-api-key (connect 7
                                {:type :api-key
                                 :params {:id key-id :api-key api_key}})
          ;; unauthorized connections
          conn-wo-auth (connect 7 nil)
          bad-conn-basic-auth (connect 7 (assoc-in basic-auth-opts [:params :pwd] "bad pwd"))
          bad-conn-api-key (connect 7
                                    {:type :api-key
                                     :params {:id key-id :api-key "bad key"}})
          test-conn (fn [conn authorized?]
                      (let [try-es #(get-template conn "*")]
                        (if authorized?
                          (is (map? (try-es)))
                          (is (thrown-with-msg? ExceptionInfo #"Unauthorized ES Request.*"
                                                (try-es))))))
          test-cases [[conn-basic-auth true]
                      [conn-api-key true]
                      [conn-wo-auth false]
                      [bad-conn-basic-auth false]
                      [bad-conn-api-key false]]]
      (doseq [[conn authorized?] test-cases]
        (test-conn conn authorized?)))))
