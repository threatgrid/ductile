(ns ductile.auth-test
  (:require [ductile.auth :as sut]
            [clojure.test :refer [deftest testing is]]))

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
         (sut/bearer {:token "any-token"}))))

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
