(ns ductile.index-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [ductile
             [index :as sut]
             [conn :as es-conn]
             [document :as es-doc]]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :once validate-schemas)

(deftest index-uri-test
  (testing "should generate a valid index URI"
    (is (= (sut/index-uri "http://127.0.0.1" "test")
           "http://127.0.0.1/test"))
    (is (= (sut/index-uri "http://127.0.0.1" "<logstash-{now/d}>")
           "http://127.0.0.1/%3Clogstash-%7Bnow%2Fd%7D%3E"))))

(deftest template-uri-test
  (testing "should generate a valid template URI"
    (is (= (sut/template-uri "http://127.0.0.1" "test")
           "http://127.0.0.1/_template/test"))
    (is (= (sut/template-uri "http://127.0.0.1" "testé")
           "http://127.0.0.1/_template/test%C3%A9"))))

(deftest rollover-uri-test
  (testing "should generate a valid rollover URI"
    (is (= (sut/rollover-uri "http://127.0.0.1" "test")
           "http://127.0.0.1/test/_rollover"))
    (is (= (sut/rollover-uri "http://127.0.0.1" "test" nil true)
           "http://127.0.0.1/test/_rollover?dry_run"))
    (is (= (sut/rollover-uri "http://127.0.0.1" "test" "test2" true)
           "http://127.0.0.1/test/_rollover/test2?dry_run"))
    (is (= (sut/rollover-uri "http://127.0.0.1" "test" "test2" false)
           "http://127.0.0.1/test/_rollover/test2"))))

(deftest refresh-uri-test
  (testing "should generat a proper refresh URI"
    (is (= (sut/refresh-uri "http://127.0.0.1" "test-index")
           "http://127.0.0.1/test-index/_refresh"))
    (is (= (sut/refresh-uri "http://127.0.0.1" nil)
           "http://127.0.0.1/_refresh"))))

(deftest ^:integration index-crud-ops
  (testing "with ES conn test setup"

    (let [conn (es-conn/connect {:host "localhost" :port 9200})]

      (testing "all ES Index CRUD operations"
        (let [base-mappings {:properties {:name {:type "text"}
                                          :age {:type "integer"}}}
              base-settings {:number_of_shards "1"
                             :number_of_replicas "1"}
              index-create-res
              (sut/create! conn
                           "test_index"
                           {:mappings base-mappings
                            :settings base-settings})
              updated-mappings (assoc-in base-mappings
                                        [:properties :email :type]
                                        "keyword")
              index-update-mappings-res (sut/update-mappings! conn
                                                              "test_index"
                                                              updated-mappings)
              updated-settings (assoc base-settings :number_of_replicas "2")
              index-update-settings-res (sut/update-settings! conn
                                                              "test_index"
                                                              {:number_of_replicas 2})
              index-get-res (sut/get conn "test_index")
              index-close-res (sut/close! conn "test_index")
              index-open-res (sut/open! conn "test_index")
              index-delete-res (sut/delete! conn "test_index")]

          (is (true? (boolean index-create-res)))
          (is (= {:test_index
                  {:aliases {}
                   :mappings updated-mappings
                   :settings
                   {:index (assoc updated-settings
                                  :provided_name "test_index")}}}
                 (update-in index-get-res
                            [:test_index :settings :index]
                            dissoc
                            :creation_date
                            :uuid
                            :version)))
          (is (= {:acknowledged true :shards_acknowledged true} index-open-res))
          (is (= {:acknowledged true
                  :shards_acknowledged true
                  :indices {:test_index {:closed true}}}
                 index-close-res))
          (is (true? (boolean index-delete-res))))))))

(deftest ^:integration rollover-test
  (let [conn (es-conn/connect {:host "localhost" :port 9200})]
    (sut/delete! conn "test_index-*")
    (sut/create! conn
                      "test_index-1"
                      {:settings {:number_of_shards 1
                                  :number_of_replicas 1}
                       :aliases {:test_alias {}}})
    (testing "rollover should not be applied if conditions are not matched"
      (let [{:keys [rolled_over dry_run new_index]}
            (sut/rollover! conn "test_alias" {:max_age "1d" :max_docs 3})]
        (is (false? rolled_over))
        (is (false? dry_run))
        (is (false? (sut/index-exists? conn new_index)))))

    (is (= {:rolled_over false :dry_run true}
           (-> (sut/rollover! conn
                                   "test_alias"
                                   {:max_age "1d" :max_docs 3}
                                   {}
                                   nil
                                   true)
               (select-keys [:rolled_over :dry_run])))
        "rollover dry_run paramater should be properly applied")

    ;; add 3 documents to trigger max-doc condition
    (es-doc/bulk-create-doc conn
                            (repeat 3 {:_index "test_alias"
                                       :foo :bar})
                            {:refresh "true"})

    (testing "rollover dry_run parameter should be properly applied when condition is met"
      (let [{:keys [rolled_over dry_run old_index new_index]}
            (sut/rollover! conn
                                "test_alias"
                                {:max_age "1d" :max_docs 3}
                                {}
                                nil
                                true)]
        (is (false? rolled_over))
        (is dry_run)
        (is (= old_index "test_index-1"))
        (is (not= new_index old_index))
        (is (false? (sut/index-exists? conn new_index)))))

    (is (= "test_index_new"
           (:new_index (sut/rollover! conn
                                           "test_alias"
                                           {:max_age "1d" :max_docs 3}
                                           {}
                                           "test_index_new"
                                           true)))
        "new_index should be equal to the name passed as parameter")

    (testing "rollover should be properly applied when condition is met and dry run set to false"
      (let [{:keys [rolled_over dry_run old_index new_index]}
            (sut/rollover! conn
                                "test_alias"
                                {:max_age "1d" :max_docs 3}
                                {:number_of_shards 2
                                 :number_of_replicas 3}
                                nil
                                false)
            {:keys [number_of_shards
                    number_of_replicas]} (get-in (sut/get conn new_index)
                                                 [(keyword new_index) :settings :index])]
        (is rolled_over)
        (is (false? dry_run))
        (is (= old_index "test_index-1"))
        (is (not= old_index new_index))
        (is (sut/index-exists? conn old_index))
        (is (sut/index-exists? conn new_index))
        (is (= "2" number_of_shards))
        (is (= "3" number_of_replicas))))

    (sut/delete! conn "test_index-*")))

(deftest template-test
  (let [conn (es-conn/connect {:host "localhost" :port 9200})
        template-name-1 "template-1"
        template-name-2 "template-2"
        config {:settings {:number_of_shards "1"
                           :refresh_interval "2s"}
                :mappings {:_source {:enabled false}}
                :aliases {:alias1 {}
                          :alias2 {:filter {:term {:user "kimchy"}}
                                   :routing "kimchy"}}}
        _  (is (= {:acknowledged true}
                  (sut/create-template! conn
                                        template-name-1
                                        config
                                        ["pattern1" "pattern2"])))
        {:keys [index_patterns mappings settings aliases]}
        (get (sut/get-template conn template-name-1)
             (keyword template-name-1))]
    (is (= (:mappings config)
           mappings))
    (is (= (:settings config)
           (:index settings)))
    (is (= {} (:alias1 aliases)))
    (is (= {:filter {:term {:user "kimchy"}}
            :index_routing "kimchy"
            :search_routing "kimchy"}
           (:alias2 aliases)))
    (is (= 2 (count aliases)))
    (is (= {:acknowledged true}
           (sut/create-template! conn
                                 template-name-2
                                 config)))
    (is (= ["template-2*"]
           (-> (sut/get-template conn template-name-2)
               (get (keyword template-name-2))
               :index_patterns)))
    (is (= {:acknowledged true}
           (sut/delete-template! conn template-name-1)))
    (is (= {:acknowledged true}
           (sut/delete-template! conn template-name-2)))
    (is (nil? (sut/get-template conn template-name-1)))
    (is (nil? (sut/get-template conn template-name-2)))))
