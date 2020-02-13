(ns ductile.conn-test
  (:require [clojure.test :refer [deftest is]]
            [ductile.conn :as sut]))

(deftest connect-test
  (is (= "https://cisco.com:9201"
         (:uri (sut/connect {:transport :https
                             :host "cisco.com"
                             :port 9201}))))
  (is (= "http://127.0.0.1:9200"
         (:uri (sut/connect {:host "127.0.0.1"
                             :port 9200})))))

(deftest mak-http-opts-test
  (let [cm (sut/make-connection-manager {})
        doc {:foo :bar
             :deeper {:for :bar}}
        opts {:refresh "true"
              :_source ["foo" "bar"]
              :param-1 "param-1"
              :param-2 "param-2"}]
  (is (= {:as :json
          :content-type :json
          :throw-exceptions false
          :connection-manager cm
          :query-params (dissoc opts :param-2)}
         (sut/make-http-opts cm
                             opts
                             [:refresh
                              :_source
                              :param-1])))
  (is (= {:as :json
          :content-type :json
          :throw-exceptions false
          :connection-manager cm
          :query-params (dissoc opts :param-2)
          :form-params doc}
         (sut/make-http-opts cm
                             opts
                             [:refresh
                              :_source
                              :param-1]
                             doc
                             nil)))
  (is (= {:as :json
          :content-type :json
          :throw-exceptions false
          :connection-manager cm
          :query-params (dissoc opts :param-2)
          :body doc}
         (sut/make-http-opts cm
                             opts
                             [:refresh
                              :_source
                              :param-1]
                             nil
                             doc)))
  (is (= {:as :json
          :content-type :json
          :throw-exceptions false
          :connection-manager cm
          :query-params {:param-1 "param-1"}}
         (sut/make-http-opts cm opts [:param-1])))
  (is (= {:as :json
          :content-type :json
          :throw-exceptions false
          :connection-manager cm}
         (sut/make-http-opts cm opts [])
         (sut/make-http-opts cm opts nil)))
  (is (= {:as :json
          :content-type :json
          :throw-exceptions false
          :query-params {:param-1 "param-1"}}
         (sut/make-http-opts nil opts [:param-1])))))
