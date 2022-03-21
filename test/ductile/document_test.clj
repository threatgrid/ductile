(ns ductile.document-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.walk :refer [keywordize-keys]]
            [clj-http.client :as client]
            [ductile.conn :as es-conn]
            [ductile.document :as sut]
            [ductile.index :as es-index]
            [ductile.query :as query]
            [ductile.test-helpers :refer [for-each-es-version]]
            [ring.util.codec :refer [form-decode]]
            [schema.test :refer [validate-schemas]])
  (:import clojure.lang.ExceptionInfo
           [java.util UUID]))

(use-fixtures :once validate-schemas)

(deftest search-uri-test
  (testing "should generate a valid _search uri"
    (is (= "http://localhost:9200/ctia_tool/_search"
           (sut/search-uri "http://localhost:9200"
                           "ctia_tool")))
    (is (= "http://localhost:9200/_search"
           (sut/search-uri "http://localhost:9200"
                           nil)))))

(deftest x-by-query-uri-test
  (testing "should generate a valid delete_by_query uri"
    (is (= "http://localhost:9200/ctim/_delete_by_query"
           (sut/delete-by-query-uri "http://localhost:9200" ["ctim"])))
    (is (= "http://localhost:9200/ctim%2Cctia/_delete_by_query"
           (sut/delete-by-query-uri "http://localhost:9200" ["ctim", "ctia"]))))
  (testing "should generate a valid update_by_query uri"
    (is (= "http://localhost:9200/ctim/_update_by_query"
           (sut/update-by-query-uri "http://localhost:9200" ["ctim"])))
    (is (= "http://localhost:9200/ctim%2Cctia/_update_by_query"
           (sut/update-by-query-uri "http://localhost:9200" ["ctim", "ctia"])))))

(deftest index-doc-uri-test
  (testing "should generate a valid doc URI"
    (is (= "http://127.0.0.1/test_index/_doc/test"
           (sut/index-doc-uri "http://127.0.0.1"
                              "test_index"
                              nil
                              "test"))
        "index-doc-uri should build an ES7 comptatible uri when the type is nil")
    (is (= "http://127.0.0.1/test_index/_doc/test"
           (sut/index-doc-uri "http://127.0.0.1"
                              "test_index"
                              ""
                              "test"))
        "index-doc-uri should buir an ES7 comptatible uri when the type is an empty string")
    (is (= "http://127.0.0.1/test-index/test-type/test-id"
           (sut/index-doc-uri "http://127.0.0.1"
                              "test-index"
                              "test-type"
                              "test-id"))
        "index-doc-uri should build ES5 document uris when a non empty type is provided")))

(deftest update-doc-uri-test
  (is (= (sut/update-doc-uri "http://127.0.0.1"
                             "test-index"
                             "test-id")
         (sut/update-doc-uri "http://127.0.0.1"
                             "test-index"
                             nil
                             "test-id")
         "http://127.0.0.1/test-index/_update/test-id"))
  (is (= (sut/update-doc-uri "http://127.0.0.1"
                             "test-index"
                             "test-type"
                             "test-id")
         "http://127.0.0.1/test-index/test-type/test-id/_update")
      "update-doc-uri should build ES5 _update uris when type is provided"))

(deftest params->pagination-test
  (is (= {:size 100
          :sort [{"field1" {:order :asc}}]}
         (sut/params->pagination {:sort_by :field1})))

  (is (= {:size 100
          :sort [{"field1" {:order :desc}}]}
         (sut/params->pagination {:sort_by "field1:desc"})
         (sut/params->pagination {:sort_by "field1:desc"
                                  :sort_order :asc})))

  (is (= {:size 100
          :sort [{"field1" {:order :desc}}
                 {"field2" {:order :asc}}
                 {"field3" {:order :desc}}]}
         (sut/params->pagination {:sort_by "field1:desc,field2:asc,field3:desc"})
         (sut/params->pagination {:sort_by "field1:desc,field2:asc,field3:desc"
                                  :sort_order :asc})))

  (is (= {:size 100
          :from 1000
          :sort [{"field1" {:order :asc}}]}
         (sut/params->pagination {:sort_by :field1
                                  :offset 1000})))

  (is (= {:size 10000
          :from 1000
          :sort [{"field1" {:order :asc}}]}
         (sut/params->pagination {:sort_by :field1
                                  :offset 1000
                                  :limit 10000})))

  (is (= {:size 10000
          :from 0
          :search_after ["value1"]
          :sort [{"field1" {:order :asc}}]}
         (sut/params->pagination {:sort_by :field1
                                  :offset 1000
                                  :limit 10000
                                  :search_after ["value1"]})
         (sut/params->pagination {:sort_by :field1
                                  :limit 10000
                                  :search_after ["value1"]}))))

(deftest generate-search-params-test
  (is (= {:size 10 :from 20}
         (sut/generate-search-params nil nil {:limit 10 :offset 20}))
      "generate-search-params should properly format pagination parameters")
  (is (= {:size 100}
         (sut/generate-search-params nil nil {}))
      "generate-search-params should apply default query values")
  (is (= {:query {:match_all {}}
          :size 10}
         (sut/generate-search-params {:match_all {}} nil {:limit 10}))
      "generate-search-params should set :query with query passed as parameter")
  (let [aggs {:docs_by_week
              {:date_histogram
               {:field "timestamp"
                :interval "week"}}}]
    (is (= {:aggs aggs :size 0}
           (sut/generate-search-params nil aggs {:limit 0}))
        "generate-search-params should set :aggs with aggs passed as parameter")))

(deftest partition-json-ops-test
  (is (= [["ops1"] ["ops2"] ["ops3--"]]
         (sut/partition-json-ops
          ["ops1" "ops2" "ops3--"]
          1))
      "All elements are in a group if the max size is exceeded")
  (is (= [["ops1" "ops2"] ["ops3--"]]
         (sut/partition-json-ops
          ["ops1" "ops2" "ops3--"]
          8))
      "The max size is used to partition ops")
  (is (= [["ops1" "ops2" "ops3--"]]
         (sut/partition-json-ops
          ["ops1" "ops2" "ops3--"]
          1000))
      "All ops are in the same group"))

(deftest ^:integration document-crud-ops
  (let [indexname (str "test_index" (UUID/randomUUID))]
    (for-each-es-version
     "all ES Document CRUD operations"
     #(es-index/delete! conn indexname)
     (let [sample-doc {:id "test_doc"
                       :foo "bar is a lie"
                       :test_value 42}
           doc-type (if (= version 5) "test-type" "_doc")
           sample-docs
           (repeatedly 10
                       #(hash-map :id (str (UUID/randomUUID))
                                  :_index indexname
                                  :bar "foo"
                                  :_type doc-type))
           get-doc (fn [doc-id opts]
                     (sut/get-doc conn indexname doc-type doc-id opts))
           get-sample-doc #(get-doc (:id sample-doc) {})

           create-doc (fn [doc opts]
                        (sut/create-doc conn indexname doc-type doc opts))

           index-doc (fn [doc opts]
                       (sut/index-doc conn indexname doc-type doc opts))

           delete-doc (fn [doc-id opts]
                        (sut/delete-doc conn indexname doc-type doc-id opts))

           update-doc (fn [doc-id doc opts]
                        (sut/update-doc conn indexname doc-type doc-id doc {}))]
       (testing "create-doc and get-doc"
         (is (nil? (get-sample-doc)))
         (is (= {:_id (-> sample-doc :id str)
                 :_type doc-type
                 :result "created"}
                (select-keys (create-doc sample-doc {:refresh "true"})
                             [:_id :_type :result])))
         (is (= sample-doc (get-sample-doc)))
         (testing "creating without id"
           (let [wo-id-doc (dissoc sample-doc :id)
                 {:keys [_id result]} (create-doc wo-id-doc {:refresh "true"})]
             (is (= "created" result))
             (is (= wo-id-doc
                    (get-doc _id {})))))
         (testing "with custom mk-id"
           (let [doc-id (str (UUID/randomUUID))
                 doc {:description "Lorem ipsum dolor sit amet"}
                 {:keys [_id result]} (create-doc doc
                                                  {:mk-id (constantly doc-id)
                                                   :refresh "true"})]
             (is (= "created" result))
             (is (= _id doc-id))
             (is (= doc
                    (get-doc doc-id {})))))
         (testing "existing doc id"
           (is (thrown? ExceptionInfo
                        (create-doc sample-doc {:refresh "true"}))))
         (testing "with field selection"
           (is (= {:foo "bar is a lie"}
                  (get-doc (:id sample-doc) {:_source ["foo"]})))))
       (testing "update-doc"
         (let [update1 {:test_value 44}
               updated-doc1 (into sample-doc update1)
               update2 {:test_value 55}
               updated-doc2 (into sample-doc update2)]
           (is (= updated-doc1
                  (update-doc (:id sample-doc)
                              update1
                              {:refresh "true"})))
           (is (= updated-doc1 (get-sample-doc)))
           (testing "with params"
             (is (= updated-doc2
                    (update-doc (:id sample-doc)
                                update2
                                {:refresh "true"
                                 :retry_on_conflict 10})))
             (is (= updated-doc2 (get-sample-doc))))))
       (testing "index-doc"
         (testing "updating a field"
           (let [indexed-doc (assoc sample-doc :test_value 66)]
             (is (= "updated"
                    (:result (index-doc indexed-doc
                                        {:refresh "true"}))))
             (is (= indexed-doc (get-sample-doc)))))
         (testing "removing a field"
           (let [indexed-doc (dissoc sample-doc :test_value)]
             (is (= "updated"
                    (:result (index-doc indexed-doc
                                        {:refresh "true"}))))
             (is (= indexed-doc (get-sample-doc)))
             ;; restore with the initial values
             (index-doc sample-doc
                        {:refresh "true"}))))
       (is (= {:data #{sample-doc (dissoc sample-doc :id)}
               :paging {:total-hits 2
                        :sort [42]}}
              (update
               (sut/search-docs conn
                                indexname
                                {:query_string {:query "bar"}}
                                {:test_value 42}
                                {:sort_by "test_value"
                                 :sort_order :desc})
               :data set)
              (update
               (sut/search-docs conn
                                indexname
                                {:query_string {:query "bar"}}
                                {:test_value 42}
                                {:sort_by "test_value"
                                 :sort_order :desc})
               :data set)))
       (is (true?
            (delete-doc (:id sample-doc)
                        {:refresh "true"})))))))

(defn rand-bulk-response
  [nb-items errors?]
  {:took 3,
   :errors errors?,
   :items (take nb-items
                [{:index
                   {:_id "1",
                    :_type "_doc",
                    :_index "test",
                    :_shards {:total 2, :successful 1, :failed 0},
                    :_primary_term 1,
                    :status 201,
                    :result "created",
                    :_version 1,
                    :_seq_no 0}}
                  {:delete
                   {:_id "2",
                    :_type "_doc",
                    :_index "test",
                    :_shards {:total 2, :successful 1, :failed 0},
                    :_primary_term 2,
                    :status 404,
                    :result "not_found",
                    :_version 1,
                    :_seq_no 1}}
                  {:create
                   {:_id "3",
                    :_type "_doc",
                    :_index "test",
                    :_shards {:total 2, :successful 1, :failed 0},
                    :_primary_term 3,
                    :status 201,
                    :result "created",
                    :_version 1,
                    :_seq_no 2}}
                  {:update
                   {:_id "1",
                    :_type "_doc",
                    :_index "test",
                    :_shards {:total 2, :successful 1, :failed 0},
                    :_primary_term 4,
                    :status 200,
                    :result "updated",
                    :_version 2,
                    :_seq_no 3}}])})

(deftest format-bulk-res-test
  (let [bulk-res-errors (rand-bulk-response 2 true)
        bulk-res-ok (rand-bulk-response 4 false)
        check-fn (fn [{:keys [msg bulk-res-list nb-items errors?]}]
                   (let [{:keys [took errors items]}
                         (sut/format-bulk-res (shuffle bulk-res-list))]
                     (is (= took (* 3 (count bulk-res-list))))
                     (is (= errors? errors))
                     (is (every? map? items))
                     (is (= nb-items (count items)))))]
    (check-fn {:msg "some errors"
               :bulk-res-list (into (repeat 2 bulk-res-errors)
                                    (repeat 4 bulk-res-ok))
               :nb-items 20
               :errors? true})
    (check-fn {:msg "no errors"
               :bulk-res-list (repeat 4 bulk-res-ok)
               :nb-items 16
               :errors? false})
    (check-fn {:msg "only errors"
               :bulk-res-list (repeat 4 bulk-res-errors)
               :nb-items 8
               :errors? true})))

(defn partition-all-2
  [coll]
  (partition-all (quot (count coll) 2) coll))

(deftest ^:integration bulk-ops
  (let [indexname (str "test_index" (UUID/randomUUID))]
    (for-each-es-version
     "all ES Document Bulk operations"
     #(es-index/delete! conn indexname)
     (let [doc-type (if (= version 5) "test-type" "_doc")
           nb-sample-docs 1000
           sample-docs (->> (repeatedly nb-sample-docs
                                        #(hash-map :id (str (UUID/randomUUID))
                                                   :_index indexname
                                                   :bar "foo"
                                                   :_type doc-type))
                            (map #(assoc % :_id (:id %))))
           [to-create-docs to-index-docs] (partition-all-2 sample-docs)
           [to-update-docs to-delete-docs] (partition-all-2 (shuffle sample-docs))
           prepared-update-docs (map #(assoc % :title "updated") to-update-docs)
           check-fn (fn [bulk-fn action action-docs filter-map]
                      (testing (format "bulk-%1$s-docs shall properly %1$s documents" (name action))
                        (let [action-filter (if (= :delete action)
                                              {}
                                              {:action (name action)})
                              prepared-docs (map #(into % action-filter) action-docs)
                              bulk-res (-> (bulk-fn conn
                                                    prepared-docs
                                                    {:refresh "true"})
                                           :items)
                              expected-result-label (case action
                                                        :create "created"
                                                        :update "updated"
                                                        :index "created"
                                                        :delete "deleted")
                              expected-search-count (cond->> (count action-docs)
                                                      (= action :delete) (- (count sample-docs)))]
                          (is (= (count bulk-res) (count action-docs)))
                          (is (every? #(= (get-in % [action :result])
                                          expected-result-label)
                                      bulk-res))
                          (is (= expected-search-count
                                 (get-in
                                  (sut/search-docs conn
                                                   indexname
                                                   {:match_all {}}
                                                   (into filter-map action-filter)
                                                   {})
                                  [:paging :total-hits]))))))]
       (check-fn sut/bulk-create-docs
                 :create
                 to-create-docs
                 {})
       (check-fn sut/bulk-index-docs
                 :index
                 to-index-docs
                 {})
       (check-fn sut/bulk-update-docs
                 :update
                 prepared-update-docs
                 {:title "updated"})
       (check-fn sut/bulk-delete-docs
                 :delete
                 to-delete-docs
                 {})
       (testing "bulk-post shall properly submit different action types in a single post"
         ;; delete/update remaining docs, recreate deleted ones
         (let [[remaining-to-update remaining-to-delete] (partition-all-2 to-update-docs)
               prepared-update-docs (map #(assoc % :title "reupdated") remaining-to-update)
               [to-recreate-docs to-reindex-docs] (partition-all (quot (count to-delete-docs) 2)
                                                                 to-delete-docs)
               bulk-actions {:create to-recreate-docs
                             :index to-reindex-docs
                             :update prepared-update-docs
                             :delete remaining-to-delete}
               bulk-res (sut/bulk conn bulk-actions {:refresh "true"})
               grouped-res (->> bulk-res
                                :items
                                (group-by ffirst))
               check-fn (fn [action filter-map]
                          (testing (format "bulk shall properly handle %s actions" action)
                            (let [expected-ids (->> (mapcat vals
                                                            (get grouped-res action))
                                                    (map :_id))
                                  ids-query (query/ids expected-ids)
                                  q (query/filter-map->terms-query filter-map ids-query)]
                              (assert (seq expected-ids))
                              (is (= (if (= action :delete) 0 (count expected-ids))
                                     (sut/count-docs conn indexname q))))))]
           (check-fn :create {})
           (check-fn :index {})
           (check-fn :update {:title "reupdated"})
           (check-fn :delete {})))))))

(deftest ^:integration search_after-consistency-test
  (let [indexname (str "test_index" (UUID/randomUUID))]
    (for-each-es-version
     "search_after must enable consitent pagination"
     #(es-index/delete! conn indexname)
     (let [docs
           (let [make-id #(str (UUID/randomUUID))]
             (map
              #(hash-map :id (make-id)
                         :foo %
                         :test "ok")
              (range 1000)))
           search-query #(get-in (sut/search-docs conn
                                                  indexname
                                                  nil
                                                  {}
                                                  {:limit 100})
                                 [:paging :sort])]
       (es-index/delete! conn indexname)
       (es-index/create! conn indexname {})
       (doseq [doc docs]
         (if (= version 5)
           (sut/create-doc conn
                           indexname
                           "doc-type"
                           doc
                           {:refresh "true"})
           (sut/create-doc conn
                           indexname
                           doc
                           {:refresh "true"})))
       (is (apply = (repeatedly 30 search-query)))))))

(deftest ^:integration count-test
  (let [indexname (str "test_index" (UUID/randomUUID))]
    (for-each-es-version
     "count-docs must properly count document"
     #(es-index/delete! conn indexname)
     (let [sample-docs (mapv #(cond-> {:_index indexname
                                       :foo :bar
                                       :_id %}
                                (= 5 version) (assoc :_type "doc-type"))
                             (range 10))]
       (es-index/delete! conn indexname)
       (es-index/create! conn indexname {})
       (sut/bulk-create-docs conn
                            sample-docs
                            {:refresh "true"})
       (is (= 10
              (sut/count-docs conn indexname {:term {:foo :bar}})
              (sut/count-docs conn indexname {:match_all {}})))
       (is (= 3 (sut/count-docs conn indexname {:ids {:values (range 3)}})))))))

(defn is-full-hits?
  [{:keys [_source _index _id]}]
  (boolean (and _source _index _id)))

(deftest ^:integration query-test
  (let [indexname (str "test_index" (UUID/randomUUID))]
    (for-each-es-version
     "query should tigger a proper search query"
     #(es-index/delete! conn indexname)
     (let [sample-docs (mapv #(cond-> {:_index indexname
                                       :foo :bar
                                       :price %
                                       :_id (str %)}
                                (= 5 version) (assoc :_type "doc-type"))
                             (range 10))
           sample-3-docs (->> (shuffle sample-docs)
                              (take 3))
           sample-3-ids (map :_id sample-3-docs)
           _ (es-index/delete! conn indexname)
           _ (es-index/create! conn indexname {})
           _ (sut/bulk-create-docs conn sample-docs {:refresh "true"})
           ids-query-result-1 (sut/query conn
                                         indexname
                                         (query/ids sample-3-ids)
                                         {})
           ids-query-result-2 (sut/query conn
                                         indexname
                                         (query/ids sample-3-ids)
                                         {:full-hits? true})
           search_after-result (sut/query conn
                                          indexname
                                          {:match_all {}}
                                          {:limit 2
                                           :sort ["price"]
                                           :search_after [5]})
           avg-aggs {:avg_price {:avg {:field :price}}}
           {data-aggs-1 :data
            aggs-1 :aggs
            paging-aggs-1 :paging} (sut/query conn
                                              indexname
                                              {:match_all {}}
                                              avg-aggs
                                              {:limit 5})

           stats-aggs {:price_stats {:stats {:field :price}}}
           {data-aggs-2 :data
            aggs-2 :aggs} (sut/query conn
                                     indexname
                                     {:match_all {}}
                                     stats-aggs
                                     {:limit 0})
           stats-aggs {:price_stats {:stats {:field :price}}}
           {data-aggs-3 :data
            aggs-3 :aggs} (sut/query conn
                                     indexname
                                     (query/ids (map :_id (take 3 sample-docs)))
                                     stats-aggs
                                     {:limit 10})]

       (is (= (repeat 3 {:foo "bar"})
              (->> ids-query-result-1
                   :data
                   (map #(select-keys % [:foo]))))
           "querying with ids query without full-hits? param should return only source of selected docs in :data")
       (testing "when full-hits is set as true, each element of :data field should contains :_id :_source and :_index fields"
         (is (= (set sample-3-ids)
                (->> (:data ids-query-result-2)
                     (map :_id)
                     set)))
         (is (= (repeat 3 "bar")
                (->> (:data ids-query-result-2)
                     (map #(-> % :_source :foo)))))
         (is (= (repeat 3 indexname)
                (->> (:data ids-query-result-2)
                     (map :_index))))
         (is (not-any? is-full-hits? (:data ids-query-result-1))
             "by default, full-hits? is set to false"))
       (testing "sort and search_after params should be properly applied"
         (is (= '(6 7)
                (map :price (:data search_after-result))))
         (is (= [7]
                (-> search_after-result :paging :sort)
                (-> search_after-result :paging :next :search_after))))
       (when (<= 7 version)
         (testing "track_total_hits should be properly considered"
           (is (= 2 (-> (sut/query conn
                                   indexname
                                   (query/ids sample-3-ids)
                                   {:track_total_hits 2})
                        :paging
                        :total-hits)))
           (is (= 3
                  (-> (sut/query conn
                                 indexname
                                 (query/ids sample-3-ids)
                                 {:track_total_hits true})
                      :paging
                      :total-hits)
                  (-> (sut/query conn
                                 indexname
                                 (query/ids sample-3-ids)
                                 {})
                      :paging
                      :total-hits)))
           (is (= 0 (-> (sut/query conn
                                   indexname
                                   (query/ids sample-3-ids)
                                   {:track_total_hits false})
                        :paging
                        :total-hits)))))
       (testing "aggs parameter should be used to perform aggregations, while applying query and paging"
         (is (= 5 (count data-aggs-1)))
         (is (= 4.5 (-> aggs-1 :avg_price :value)))
         (is (= {:total-hits 10
                 :next {:limit 5 :offset 5}}
                paging-aggs-1))
         (is (= 0 (count data-aggs-2)))
         (is (= {:count 10
                 :min 0.0
                 :max 9.0
                 :avg 4.5
                 :sum 45.0}
                (:price_stats aggs-2)))
         (is (= 3 (count data-aggs-3)))
         (is (= {:count 3
                 :min 0.0
                 :max 2.0
                 :avg 1.0
                 :sum 3.0}
                (:price_stats aggs-3))))))))

(deftest ^:integration delete-by-query-test
  (let [indexname (str "test_index" (UUID/randomUUID))
        indexname1 (str indexname "-1")
        indexname2 (str indexname "-2")]
    (for-each-es-version
     "delete-by-query should trigger a proper _delete_by_query request"
     #(es-index/delete! conn (str indexname "*"))
     (let [sample-docs-1 (mapv #(cond-> {:_index indexname1
                                         :foo (if (< % 5)
                                                :bar1
                                                :bar2)
                                         :_id %}
                                  (= 5 version) (assoc :_type "doc-type"))
                               (range 10))
           sample-docs-2 (mapv #(cond-> {:_index indexname2
                                         :foo (if (< % 5)
                                                :bar1
                                                :bar2)
                                         :_id %}
                                  (= 5 version) (assoc :_type "doc-type"))
                               (range 10))
           q-term (query/term :foo :bar2)
           q-ids-1 (query/ids ["0" "1" "2"])
           q-ids-2 (query/ids ["3" "4"])]
       (es-index/delete! conn indexname)
       (es-index/create! conn indexname {})
       (sut/bulk-create-docs conn sample-docs-1 {:refresh "true"})
       (sut/bulk-create-docs conn sample-docs-2 {:refresh "true"})
       (is (= 5
              (:deleted (sut/delete-by-query conn
                                             [indexname1]
                                             q-term
                                             {:wait_for_completion true
                                              :refresh "true"})))
           "delete-by-query should delete all documents that match a query for given index")
       (is (= 6
              (:deleted (sut/delete-by-query conn
                                             [indexname1, indexname2]
                                             q-ids-1
                                             {:wait_for_completion true
                                              :refresh "true"})))
           "delete-by-query should properly apply deletion on all given indices")
       (is (seq (:task (sut/delete-by-query conn
                                            [indexname1, indexname2]
                                            q-ids-2
                                            {:wait_for_completion false
                                             :refresh "true"})))
           "delete-by-query with wait-for-completion? set to false should directly return an answer before deletion with a task id")))))

(deftest ^:integration update-by-query-test
  (let [indexname "test_index"]
    (for-each-es-version
     "update by query."
     #(es-index/delete! conn indexname)
     (let [;; init state
           doc-type (when (= 5 version) "sighting")
           base-mappings (cond->> {:dynamic false ;; do not index fields without mapping
                                   :properties {:name {:type "keyword"}
                                                :age {:type "integer"}
                                                :title {:type "text"}}}
                           (= version 5) (assoc {} doc-type)) ;; ES5/7 mapping compatilbity
           base-settings {:number_of_shards "1"
                          :number_of_replicas "1"}
           index-create-res (es-index/create!
                             conn
                             indexname
                             {:mappings base-mappings
                              :settings base-settings})
           _ (assert (true? (boolean index-create-res))
                     "the test index was not properly initialized")

           ;; insert some documents
           sample-docs (map #(hash-map :_index indexname
                                       :_id (str (UUID/randomUUID))
                                       :_type doc-type
                                       :name (str "name " %)
                                       :age %
                                       ;; one more field that's not indexed yet
                                       :sport "boxing")
                            (range 20))
           _ (sut/bulk-create-docs
              conn
              sample-docs
              {:refresh "true"})]
       (testing "filter on a query and update with a script"
         (let [query-fn #(sut/search-docs
                          conn
                          indexname
                          {:query_string {:query "title:young"}}
                          {} {})]
           (is (= 0 (->> (query-fn) :data count))
               "no records with title:young at this point")
           (is (= 10 (-> (sut/update-by-query
                          conn
                          [indexname]
                          {:script {:source "ctx._source.title=\"young\""}
                           :query {:range {:age {:lt 10}}}}
                          {:refresh "true"})
                         :updated))
               "expected to update exact number of records")
           (is (= 10 (->> (query-fn)
                          :data
                          count))
               "selected records have gotten updated")))
       (testing "pick new properties"
         (let [query-fn #(sut/query
                          conn
                          indexname
                          {:match {"sport" "boxing"}} {})]
           ;; since :sport field mapping doesn't exist yet, there should be no data
           (is (= 0 (->> (query-fn) :data count)))

           ;; update the mappings, to include :sport field
           (es-index/update-mappings!
            conn
            indexname
            doc-type
            (cond->> {:properties {:sport {:type "text"}}}
              (= version 5) (assoc {} doc-type)))

           ;; since mapping was updated _after_ we inserted data, :sport field still
           ;; not indexed, and searching on that field still shouldn't get anything
           (is (= 0 (->> (query-fn) :data count)))

           ;; now we force ES to pick up properties that were added _after_ the data
           ;; was initially inserted
           (sut/update-by-query
            conn
            [indexname] {}
            {:refresh "true"})

           ;; and finally, :sport field is properly indexed and appears when searched
           (is (= 20 (->> (query-fn) :data count)))))))))

(deftest query-params-test
  (let [;; Note: index not created in this test
        indexname (str "test_index" (UUID/randomUUID))
        query-params (atom nil)
        check-query-params (fn [expected msg]
                             (is (= @query-params expected)
                                 msg)
                             (reset! query-params nil))
        conn (es-conn/connect {:host "localhost"
                               :port 9200
                               :request-fn
                               (-> (fn [req]
                                     (reset! query-params (keywordize-keys (form-decode (:query-string req))))
                                     {:status 200
                                      :headers {:content-type "application/clojure"}})
                                   client/wrap-query-params)})]
    (testing "update query parameter"
      (sut/update-doc conn indexname "1" {:foo "bar"} {})
      (check-query-params {:_source "true"
                           :retry_on_conflict "5"}
                          "default opts should be properly set in update-doc")
      (sut/update-doc conn
                      indexname
                      "1"
                      {:foo "bar"}
                      {:refresh "wait_for"
                       :_source ["foo" "label"]
                       :retry_on_conflict 2
                       :to-be-ignored "ignored"})
      (check-query-params {:_source ["foo" "label"]
                           :retry_on_conflict "2"
                           :refresh "wait_for"}
                          "accepted query-params options are `_source`, `refresh`, and `retry_on_conflict`"))
    (testing "get-doc query parameters"
      (sut/get-doc conn indexname "1" {})
      (check-query-params ""
                          "no default query-params")
      (sut/get-doc conn indexname "1" {:_source ["foo" "bar"]
                                          :to-be-ignored "ignored"})
      (check-query-params {:_source ["foo" "bar"]}
                          "only `_source` is accepted as query-parmas"))
    (testing "create-doc query parameters"
      (sut/create-doc conn indexname {:a "doc"} {})
      (check-query-params ""
                          "create-doc should not set `op_type` when id is missing")
      (sut/create-doc conn indexname {:a "doc"} {:mk-id :a})
      (check-query-params {:op_type "create"}
                          "create-doc should set `op_type` to force error when document exists")
      (sut/create-doc conn indexname {:a "doc"} {:mk-id :a
                                                    :refresh "wait_for"
                                                    :to-be-ignored "ignored"})
      (check-query-params {:op_type "create"
                           :refresh "wait_for"}
                          "`refresh` is the only accepted query parameter"))
    (testing "index-doc query parameters"
      (sut/index-doc conn indexname {:a "doc"} {})
      (check-query-params "" "index-doc has no default query parameter")
      (sut/index-doc conn
                     indexname
                     {:a "doc"}
                     {:mk-id :a
                      :op_type "index"
                      :refresh "true"
                      :to-be-ignored "ignored"})
      (check-query-params {:op_type "index"
                           :refresh "true"}
                          "accepted query parameter are `refresh` and `op_type"))
    (testing "delete-doc query parameters"
      (sut/delete-doc conn indexname "1" {})
      (check-query-params "" "delete-doc has no default query parameter")
      (sut/delete-doc conn indexname "1" {:refresh "wait_for"
                                          :to-be-ignored "ignored"})
      (check-query-params {:refresh "wait_for"}
                          "`refresh` is the only accepted query parameter"))))

(deftest pagination-params-test
  (testing "pagination parameters must be properly extracted from result query and query parameter"
    (let [pagination-params #'sut/pagination-params
          es-params {:from 20
                     :size 10
                     :search_after ["id-20"]}
          hits (map #(let [title (str "title 2" %)
                           id (str "id-2" %)]
                       {:_source {:title title
                                  :id id}
                        :sort [id]})
                    (range 10))
          es5-result {:hits {:total 10
                             :hits hits}}
          es7-result {:hits {:total {:value 10}
                             :hits hits}}]
      (is (= {:offset 20
              :limit 10
              :sort ["id-29"]
              :search_after ["id-20"]
              :total-hits 10}
             (pagination-params es5-result
                                es-params)
             (pagination-params es7-result
                                es-params))))))
