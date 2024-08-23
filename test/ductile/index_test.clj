(ns ductile.index-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.set :as set]
            [clojure.string :as string]
            [ductile.document :as es-doc]
            [ductile.index :as sut]
            [ductile.test-helpers :refer [for-each-es-version]]
            [schema.test :refer [validate-schemas]])
  (:import java.util.UUID clojure.lang.ExceptionInfo))

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

(deftest index-template-uri-test
  (testing "should generate a valid index template URI"
    (is (= (sut/index-template-uri "http://127.0.0.1" "test")
           "http://127.0.0.1/_index_template/test"))
    (is (= (sut/index-template-uri "http://127.0.0.1" "testé")
           "http://127.0.0.1/_index_template/test%C3%A9"))))

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

(deftest policy-uri-test
  (testing "should generate a proper policy URI"
    (is (= (sut/policy-uri "http://127.0.0.1" "test-policy")
           "http://127.0.0.1/_ilm/policy/test-policy"))))

(deftest data-stream-uri-test
  (testing "should generate a proper data stream URI"
    (is (= (sut/data-stream-uri "http://127.0.0.1" "test-data-stream")
           "http://127.0.0.1/_data_stream/test-data-stream"))))

(deftest ^:integration policy-test
  (let [policy-name "test-policy"
        policy {:phases
                {:hot
                 {:min_age "0ms"
                  :actions
                  {:rollover {:max_docs 100000000}}}}}]
    (for-each-es-version
     "Policy operations should be available for ES >= 7"
     (when (< 7 version) (sut/delete-policy! conn policy-name))
     (if (< version 7)
       (do
         (is (thrown? ExceptionInfo
                      (sut/create-policy! conn policy-name policy)))
         (is (thrown? ExceptionInfo
                      (sut/delete-policy! conn policy-name)))
         (is (thrown? ExceptionInfo
                      (sut/get-policy conn policy-name))))
       (do
         (is (= {:acknowledged true}
                (sut/create-policy! conn policy-name policy)))
         (is (= policy
                (get-in (sut/get-policy conn policy-name)
                        [(keyword policy-name) :policy])))
         (is (= {:acknowledged true}
                (sut/delete-policy! conn policy-name)))
         (is (= nil (sut/get-policy conn policy-name))))))))

(deftest ^:integration index-crud-ops
  (let [indexname "test_index"
        indexkw (keyword indexname)]
    (for-each-es-version
     "all ES Index CRUD operations"
     #(sut/delete! conn indexname)
     (let [doc-type (when (= 5 version) :sighting)
           base-mappings (cond->> {:properties {:name {:type "text"}
                                                :age {:type "integer"}}}
                           (= version 5) (assoc {} doc-type))
           base-settings {:number_of_shards "1"
                          :number_of_replicas "1"}
           index-create-res
           (sut/create! conn
                        indexname
                        {:mappings base-mappings
                         :settings base-settings})
           updated-mappings (assoc-in base-mappings
                                      (remove nil? [doc-type :properties :email :type])
                                      "keyword")
           _ (sut/update-mappings! conn
                                   indexname
                                   (some-> doc-type name)
                                   updated-mappings)
           updated-settings (assoc base-settings :number_of_replicas "2")
           _ (sut/update-settings! conn
                                   indexname
                                   {:number_of_replicas 2})
           index-get-res (sut/get conn indexname)
           index-close-res (sut/close! conn indexname)
           index-open-res (sut/open! conn indexname)
           index-delete-res (sut/delete! conn indexname)]

       (is (true? (boolean index-create-res)))
       (is (= {indexkw
               {:aliases {}
                :mappings updated-mappings
                :settings
                {:index (assoc updated-settings
                               :provided_name indexname)}}}
              (update-in index-get-res
                         [indexkw :settings :index]
                         select-keys
                         [:number_of_shards
                          :number_of_replicas
                          :provided_name])))
       (is (= (cond-> {:acknowledged true}
                (< 5 version) (assoc :shards_acknowledged true))
              index-open-res))
       (is (= (cond-> {:acknowledged true}
                (< 5 version) (assoc :shards_acknowledged true
                                     :indices {indexkw {:closed true}}))
              index-close-res))
       (is (true? (boolean index-delete-res)))))))

(deftest ^:integration rollover-test
  (let [indexname (str "test_index" (UUID/randomUUID))
        indexname1 (str indexname "-1")
        new-indexname (str indexname "_new")
        aliasname (str "test_alias" (UUID/randomUUID))
        aliaskw (keyword aliasname)]
    (for-each-es-version
     "rollover should properly trigger _rollover"
     #(do (sut/delete! conn (str indexname "-*"))
          (sut/delete! conn (str new-indexname "*")))
     (sut/create! conn
                  indexname1
                  {:settings {:number_of_shards 1
                              :number_of_replicas 1}
                   :aliases {aliaskw {}}})
     (testing "rollover should not be applied if conditions are not matched"
       (let [{:keys [rolled_over dry_run new_index]}
             (sut/rollover! conn aliasname {:max_age "1d" :max_docs 3} {})]
         (is (false? rolled_over))
         (is (false? dry_run))
         (is (false? (sut/index-exists? conn new_index)))))

     (is (= {:rolled_over false :dry_run true}
            (-> (sut/rollover! conn
                               aliasname
                               {:max_age "1d" :max_docs 3}
                               {:dry_run true})
                (select-keys [:rolled_over :dry_run])))
         "rollover dry_run paramater should be properly applied")

     ;; add 3 documents to trigger max-doc condition
     (es-doc/bulk-index-docs conn
                             (repeat 3 (cond-> {:_index aliasname
                                                :foo :bar}
                                         (= 5 version) (assoc :_type "doc_type")))
                             {:refresh "true"})

     (testing "rollover dry_run parameter should be properly applied when condition is met"
       (let [{:keys [rolled_over dry_run old_index new_index]}
             (sut/rollover! conn
                            aliasname
                            {:max_age "1d" :max_docs 3}
                            {:dry_run true})]
         (is (false? rolled_over))
         (is dry_run)
         (is (= old_index indexname1))
         (is (not= new_index old_index))
         (is (false? (sut/index-exists? conn new_index)))))

     (is (= new-indexname
            (:new_index (sut/rollover! conn
                                       aliasname
                                       {:max_age "1d" :max_docs 3}
                                       {:new-index-name new-indexname
                                        :dry_run true})))
         "new_index should be equal to the name passed as parameter")

     (testing "rollover should be properly applied when condition is met and dry run set to false"
       (let [{:keys [rolled_over dry_run old_index new_index]}
             (sut/rollover! conn
                            aliasname
                            {:max_age "1d" :max_docs 3}
                            {:new-index-settings {:number_of_shards 2
                                                  :number_of_replicas 3}
                             :dry_run false})
             {:keys [number_of_shards
                     number_of_replicas]} (get-in (sut/get conn new_index)
                                                  [(keyword new_index) :settings :index])]
         (is rolled_over)
         (is (false? dry_run))
         (is (= old_index indexname1))
         (is (not= old_index new_index))
         (is (sut/index-exists? conn old_index))
         (is (sut/index-exists? conn new_index))
         (is (= "2" number_of_shards))
         (is (= "3" number_of_replicas)))))))

(deftest ^:integration template-test
  (for-each-es-version
   "template crud operations should trigger valid _template requests"
   nil
   (let [template-name-1 "template-1"
         template-name-2 "template-2"
         doc-type :malware
         alias1 :alias1
         alias2 :alias2
         config {:settings {:number_of_shards "1"
                            :refresh_interval "2s"}
                 :mappings (cond->> {:_source {:enabled false}}
                             (= version 5) (assoc {} doc-type))
                 :aliases {alias1 {}
                           alias2 {:filter {:term {:user "kimchy"}}
                                   :routing "kimchy"}}}
         _  (is (= {:acknowledged true}
                   (sut/create-template! conn
                                         template-name-1
                                         config
                                         ["pattern1" "pattern2"])))
         {:keys [template index_patterns mappings settings aliases]}
         (get (sut/get-template conn template-name-1)
              (keyword template-name-1))]
     (is (= (:mappings config)
            mappings))
     (is (= (:settings config)
            (:index settings)))
     (is (= {} (alias1 aliases)))
     (is (= {:filter {:term {:user "kimchy"}}
             :index_routing "kimchy"
             :search_routing "kimchy"}
            (alias2 aliases)))
     (is (= 2 (count aliases)))
     (if (= version 5)
       (is (= template "pattern1"))
       (is (= index_patterns ["pattern1" "pattern2"])))
     (is (= {:acknowledged true}
            (sut/create-template! conn
                                  template-name-2
                                  config)))
     (let [template (-> (sut/get-template conn template-name-2)
                        (get (keyword template-name-2)))]
       (if (= version 5)
         (is (= "template-2*" (:template template)))
         (is (= ["template-2*"] (:index_patterns template)))))
     (is (= {:acknowledged true}
            (sut/delete-template! conn template-name-1)))
     (is (= {:acknowledged true}
            (sut/delete-template! conn template-name-2)))
     (is (nil? (sut/get-template conn template-name-1)))
     (is (nil? (sut/get-template conn template-name-2))))))

(deftest ^:integration index-template-test
  (for-each-es-version
   "index-template crud operations should trigger valid _index_template requests"
   nil
   (let [template-name "template-test"
         alias1 :alias1
         alias2 :alias2
         index-template {:index_patterns ["pattern1" "pattern2"]
                         :template {:settings {:number_of_shards "1"
                                               :refresh_interval "2s"}
                                    :mappings {:_source {:enabled false}}
                                    :aliases {alias1 {}
                                              alias2 {:filter {:term {:user "kimchy"}}
                                                      :routing "kimchy"}}}}]
     (if (< version 7)
       (do
         (is (thrown? ExceptionInfo
                      (sut/create-index-template! conn template-name index-template)))
         (is (thrown? ExceptionInfo
                      (sut/get-index-template conn template-name)))
         (is (thrown? ExceptionInfo
                      (sut/delete-index-template! conn template-name))))
       (let [_ (is (= {:acknowledged true}
                      (sut/create-index-template! conn
                                                  template-name
                                                  index-template)))]
         (is (= {:index_templates
                 [{:name "template-test",
                   :index_template
                   {:index_patterns ["pattern1" "pattern2"],
                    :template
                    {:settings
                     {:index {:number_of_shards "1", :refresh_interval "2s"}},
                     :mappings {:_source {:enabled false}},
                     :aliases
                     {:alias1 {},
                      :alias2
                      {:filter {:term {:user "kimchy"}},
                       :index_routing "kimchy",
                       :search_routing "kimchy"}}},
                    :composed_of []}}]}
                (sut/get-index-template conn template-name)))
         (is (= {:acknowledged true}
                (sut/delete-index-template! conn template-name)))
         (is (nil? (sut/get-index-template conn template-name))))))))

(deftest ^:integration data-stream-test
  (let [data-stream-name "test-data-stream"
        index-template {:index_patterns [(str data-stream-name "*")]
                        :data_stream {}
                        :template {:settings {:number_of_shards "1"
                                              :number_of_replicas "0"}}}]
    (for-each-es-version
     "data-stream operations should be available for ES >= 7"
     (when (< 7 version) (sut/delete-data-stream! conn data-stream-name))
     (if (< version 7)
       (do
         (is (thrown? ExceptionInfo
                      (sut/create-data-stream! conn data-stream-name)))
         (is (thrown? ExceptionInfo
                      (sut/delete-data-stream! conn data-stream-name)))
         (is (thrown? ExceptionInfo
                      (sut/get-data-stream conn data-stream-name))))
       (do
         (assert (= {:acknowledged true}
                    (sut/create-index-template! conn
                                                data-stream-name
                                                index-template))
                 "data-stream must match an index template in order to be created")
         (is (= {:acknowledged true}
                (sut/create-data-stream! conn data-stream-name)))
         (is (= data-stream-name
                (get-in (sut/get-data-stream conn data-stream-name)
                        [:data_streams 0 :name])))
         (is (= {:acknowledged true}
                (sut/delete-data-stream! conn data-stream-name)))
         (is (= nil (sut/get-data-stream conn data-stream-name))))))))

(deftest ^:integration cat-indices-test
  (for-each-es-version
   "cat-indices shall properly return indices data"
   #(sut/delete! conn "cat-indices-test-*")
   (let [indices-names (into #{}
                             (map #(str "cat-indices-test-" %))
                             (range 10))]
     (doseq [indexname (seq indices-names)]
       (sut/create! conn indexname {}))
     (let [cat-res (filter
                    #(string/starts-with? (:index %) "cat-indices-test")
                    (sut/cat-indices conn))]
       (is (= 10 (count cat-res)))
       (is (set/subset? indices-names (set (map :index cat-res))))
       (is (every? zero? (map :docs.count cat-res)))))))

(deftest ^:integration fetch-test
  (let [indices (cons "settings-test-fetch"
                      (map #(str "settings-test-fetch-" %) (range 10)))]
    (for-each-es-version
     "fetch shall properly return settings"
     #(sut/delete! conn "settings-test-*")

     (doseq [index indices]
       (sut/create! conn index {}))

     (testing "fetch settings of specific index"
       (let [{:keys [settings-test-fetch]} (sut/get-settings conn "settings-test-fetch")]
         (is (= 10000 (get-in settings-test-fetch [:index :max_result_window]))))
       (sut/update-settings! conn "settings-test-fetch" {:max_result_window 20})
       (let [{:keys [settings-test-fetch]} (sut/get-settings conn)]
         (is (= 20 (get-in settings-test-fetch [:index :max_result_window])))))

     (testing "fetch settings of all indices"
       (let [res (sut/get-settings conn)]
         (is (= (set (map keyword indices))
                (set (keys res)))))))))

(deftest ^:integration alias-actions-test
  (let [indexname "test_index"
        indexkw (keyword indexname)]
    (for-each-es-version
     "all ES Index CRUD operations"
     #(sut/delete! conn indexname)
     (let [doc-type (when (= 5 version) :sighting)
           base-mappings (cond->> {:properties {:name {:type "text"}
                                                :age {:type "integer"}}}
                           (= version 5) (assoc {} doc-type))
           base-settings {:number_of_shards "1"
                          :number_of_replicas "1"}
           index-create-res
           (sut/create! conn
                        indexname
                        {:mappings base-mappings
                         :settings base-settings})
           test-cases [{:message "add aliases"
                        :actions [{:add {:index indexname :alias "alias1"}}
                                  {:add {:index indexname :alias "alias2"}}]}
                       {:message "remove and add"
                        :actions [{:remove {:index indexname :alias "alias1"}}
                                  {:add {:index indexname :alias "alias3"}}]}
                       {:message "add is_write_index to an existing index and add a new one"
                        :es-versions #{7}
                        :actions [{:add {:index indexname :alias "alias3" :is_write_index true}}
                                  {:add {:index indexname :alias "alias4" :is_write_index true}}]}]
           check-action (fn [action]
                          (let [[action-type action-params] (first action)
                                index (sut/get conn indexname)
                                index-alias (first (filter #(= (name (first %)) (:alias action-params))
                                                           (get-in index [indexkw :aliases])))]
                            (case action-type
                              :add (is (seq index-alias))
                              :remove (is (nil? index-alias)))))]
       (doseq [{:keys [message es-versions actions]
                :or {es-versions #{5 7}}} test-cases]
         (when (contains? (set es-versions) version)
           (testing message
             (is (= {:acknowledged true}
                    (sut/alias-actions! conn actions)))
             (doseq [action actions]
               (check-action action)))))))))
