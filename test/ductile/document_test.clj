(ns ductile.document-test
  (:import clojure.lang.ExceptionInfo)
  (:require [ductile
             [document :as sut]
             [conn :as es-conn]
             [index :as es-index]
             [query :as query]]
            [ring.util.codec :refer [form-decode]]
            [clojure.walk :refer [keywordize-keys]]
            [cheshire.core :as json]
            [schema.test :refer [validate-schemas]]
            [clj-http.fake :refer [with-fake-routes-in-isolation]]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.string :as string]
            [ductile.conn :as conn]))

(use-fixtures :once validate-schemas)

(deftest search-uri-test
  (testing "should generate a valid _search uri"
    (is (= "http://localhost:9200/ctia_tool/_search"
           (sut/search-uri "http://localhost:9200"
                              "ctia_tool")))
    (is (= "http://localhost:9200/_search"
           (sut/search-uri "http://localhost:9200"
                              nil)))))

(deftest delete-by-query-uri-test
  (testing "should generate a valid delete_by_query uri"
    (is (= "http://localhost:9200/ctim/_delete_by_query"
           (sut/delete-by-query-uri "http://localhost:9200"
                                       ["ctim"])))
    (is (= "http://localhost:9200/ctim%2Cctia/_delete_by_query"
           (sut/delete-by-query-uri "http://localhost:9200"
                                       ["ctim", "ctia"])))))

(deftest index-doc-uri-test
  (testing "should generate a valid doc URI"
    (is (= "http://127.0.0.1/test_index/_doc/test"
           (sut/index-doc-uri "http://127.0.0.1"
                                  "test_index"
                                  "test")))
    (is (= "http://127.0.0.1/test_index/_doc/test%2Ffoo%2Fbar"
           (sut/index-doc-uri "http://127.0.0.1"
                                  "test_index"
                                  "test/foo/bar")))))

(deftest update-doc-uri-test
  (is (= (sut/update-doc-uri "http://127.0.0.1"
                                "test_index"
                                "test")
         "http://127.0.0.1/test_index/_update/test"))  )


(deftest params->pagination-test
  (is (= {:size 100
          :sort {"field1" {:order :asc}}}
         (sut/params->pagination {:sort_by :field1})))

  (is (= {:size 100
          :sort {"field1" {:order :desc}}}
         (sut/params->pagination {:sort_by "field1:desc"})
         (sut/params->pagination {:sort_by "field1:desc"
                                     :sort_order :asc})))

  (is (= {:size 100
          :sort {"field1" {:order :desc}
                 "field2" {:order :asc}
                 "field3" {:order :desc}}}
         (sut/params->pagination {:sort_by "field1:desc,field2:asc,field3:desc"})
         (sut/params->pagination {:sort_by "field1:desc,field2:asc,field3:desc"
                                     :sort_order :asc})))

  (is (= {:size 100
          :from 1000
          :sort {"field1" {:order :asc}}}
         (sut/params->pagination {:sort_by :field1
                                     :offset 1000})))

  (is (= {:size 10000
          :from 1000
          :sort {"field1" {:order :asc}}}
         (sut/params->pagination {:sort_by :field1
                                     :offset 1000
                                     :limit 10000})))

  (is (= {:size 10000
          :from 0
          :search_after ["value1"]
          :sort {"field1" {:order :asc}}}
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

(deftest ^:integration document-crud-ops
  (testing "with ES conn test setup"
    (let [conn (es-conn/connect {:host "localhost" :port 9200})]

      (es-index/delete! conn "test_index")

      (testing "all ES Document CRUD operations"
        (let [sample-doc {:id "test_doc"
                          :foo "bar is a lie"
                          :test_value 42}
              sample-docs
              (repeatedly 10 #(hash-map :id (.toString (java.util.UUID/randomUUID))
                                        :_index "test_index"
                                        :bar "foo"))
              get-sample-doc #(sut/get-doc conn
                                           "test_index"
                                           (:id sample-doc)
                                           {})]
          (testing "create-doc and get-doc"
            (is (nil? (get-sample-doc)))
            (is (= {:_id (-> sample-doc :id str)
                    :_type "_doc"
                    :result "created"}
                   (select-keys (sut/create-doc conn
                                                "test_index"
                                                sample-doc
                                                {:refresh "true"})
                                [:_id :_type :result])))
            (is (= sample-doc (get-sample-doc)))
            (testing "creating without id"
              (let [wo-id-doc (dissoc sample-doc :id)
                    {:keys [_id result]} (sut/create-doc conn
                                                         "test_index"
                                                         wo-id-doc
                                                         {:refresh "true"})]
                (is (= "created" result))
                (is (= wo-id-doc
                       (sut/get-doc conn
                                    "test_index"
                                    _id
                                    {})))))
            (testing "with custom mk-id"
              (let [doc-id (str (java.util.UUID/randomUUID))
                    doc {:description "Lorem ipsum dolor sit amet"}
                    {:keys [_id result]} (sut/create-doc conn
                                                         "test_index"
                                                         doc
                                                         {:mk-id (constantly doc-id)
                                                          :refresh "true"})]
                (is (= "created" result))
                (is (= _id doc-id))
                (is (= doc
                       (sut/get-doc conn
                                    "test_index"
                                    doc-id
                                    {})))))
            (testing "existing doc id"
              (is (thrown? ExceptionInfo
                           (sut/create-doc conn
                                           "test_index"
                                           sample-doc
                                           {:refresh "true"}))))
            (testing "with field selection"
              (is (= {:foo "bar is a lie"}
                     (sut/get-doc conn
                                     "test_index"
                                     (:id sample-doc)
                                     {:_source ["foo"]})))))
          (testing "update-doc"
            (let [update1 {:test_value 44}
                  updated-doc1 (into sample-doc update1)
                  update2 {:test_value 55}
                  updated-doc2 (into sample-doc update2)]
              (is (= updated-doc1
                     (sut/update-doc conn
                                     "test_index"
                                     (:id sample-doc)
                                     update1
                                     {:refresh "true"})))
              (is (= updated-doc1 (get-sample-doc)))
              (testing "with params"
                (is (= updated-doc2
                       (sut/update-doc conn
                                       "test_index"
                                       (:id sample-doc)
                                       update2
                                       {:refresh "true"
                                        :retry_on_conflict 10})))
                (is (= updated-doc2 (get-sample-doc))))))
          (testing "index-doc"
            (testing "updating a field"
              (let [indexed-doc (assoc sample-doc :test_value 66)]
                (is (= "updated"
                       (:result (sut/index-doc conn
                                               "test_index"
                                               indexed-doc
                                               {:refresh "true"}))))
                (is (= indexed-doc (get-sample-doc)))))
            (testing "removing a field"
              (let [indexed-doc (dissoc sample-doc :test_value)]
                (is (= "updated"
                       (:result (sut/index-doc conn
                                               "test_index"
                                               indexed-doc
                                               {:refresh "true"}))))
                (is (= indexed-doc (get-sample-doc)))
                ;; restore with the initial values
                (sut/index-doc conn
                               "test_index"
                               sample-doc
                               {:refresh "true"}))))
          (testing "bulk-create-doc"
            (is (= sample-docs
                   (sut/bulk-create-doc conn
                                        sample-docs
                                        {:refresh "true"})))
            (testing "with partioning"
              (let [sample-docs-2 (map #(assoc % :test_value 43) sample-docs)]
                (is (= sample-docs-2
                       (sut/bulk-create-doc conn
                                            sample-docs-2
                                            {:refresh "true"}
                                            0)))
                (is (= 10
                       (get-in (sut/search-docs conn
                                                   "test_index"
                                                   {:query_string {:query "*"}}
                                                   {:test_value 43}
                                                   {:sort_by "test_value"
                                                    :sort_order :desc})
                               [:paging :total-hits]))))))
          (is (= {:data #{sample-doc (dissoc sample-doc :id)}
                  :paging {:total-hits 2
                           :sort [42]}}
                 (update
                  (sut/search-docs conn
                                   "test_index"
                                   {:query_string {:query "bar"}}
                                   {:test_value 42}
                                   {:sort_by "test_value"
                                    :sort_order :desc})
                  :data set)
                 (update
                  (sut/search-docs conn
                                   "test_index"
                                   {:query_string {:query "bar"}}
                                   {:test_value 42}
                                   {:sort_by "test_value"
                                    :sort_order :desc})
                  :data set)))
          (is (true?
               (sut/delete-doc conn
                               "test_index"
                               (:id sample-doc)
                               {:refresh "true"})))))

      (es-index/delete! conn "test_index"))))

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

(deftest ^:integration search_after-consistency-test
  (let [docs
        (let [make-id #(.toString (java.util.UUID/randomUUID))]
          (map
           #(hash-map :id (make-id)
                      :foo %
                      :test "ok")
           (range 1000)))
        conn (es-conn/connect {:host "localhost" :port 9200})
        search-query #(get-in (sut/search-docs conn
                                                  "test_index"
                                                  nil
                                                  {}
                                                  {:limit 100})
                              [:paging :sort])]
    (es-index/delete! conn "test_index")
    (es-index/create! conn "test_index" {})
    (doseq [doc docs]
      (sut/create-doc conn
                         "test_index"
                         doc
                         {:refresh "true"}))
    (is (apply = (repeatedly 30 search-query)))
    (es-index/delete! conn "test_index")))

(deftest ^:integration count-test
  (let [sample-docs (mapv #(assoc {:_index "test_index"
                                   :foo :bar}
                                  :_id %)
                          (range 10))
        conn (es-conn/connect {:host "localhost" :port 9200})]
    (es-index/delete! conn "test_index")
    (es-index/create! conn "test_index" {})
    (sut/bulk-create-doc conn
                         sample-docs
                         {:refresh "true"})
    (is (= 10
           (sut/count-docs conn "test_index")
           (sut/count-docs conn "test_index")
           (sut/count-docs conn "test_index" {:term {:foo :bar}})
           (sut/count-docs conn "test_index" {:match_all {}})))
    (is (= 3 (sut/count-docs conn "test_index" {:ids {:values (range 3)}})))
    (es-index/delete! conn "test_index")))

(defn is-full-hits?
  [{:keys [_source _index _id]}]
  (boolean (and _source _index _id)))

(deftest ^:integration query-test
  (let [sample-docs (mapv #(assoc {:_index "test_index"
                                   :foo :bar
                                   :price %}
                                  :_id (str %))
                          (range 10))
        conn (es-conn/connect {:host "localhost" :port 9200})
        sample-3-docs (->> (shuffle sample-docs)
                           (take 3))
        sample-3-ids (map :_id sample-3-docs)
        _ (es-index/delete! conn "test_index")
        _ (es-index/create! conn "test_index" {})
        _ (sut/bulk-create-doc conn sample-docs {:refresh "true"})
        ids-query-result-1 (sut/query conn
                                         "test_index"
                                         (query/ids sample-3-ids)
                                         {})
        ids-query-result-2 (sut/query conn
                                         "test_index"
                                         (query/ids sample-3-ids)
                                         {:full-hits? true})
        search_after-result (sut/query conn
                                          "test_index"
                                          {:match_all {}}
                                          {:limit 2
                                           :sort ["price"]
                                           :search_after [5]})
        avg-aggs {:avg_price {:avg {:field :price}}}
        {data-aggs-1 :data
         aggs-1 :aggs
         paging-aggs-1 :paging} (sut/query conn
                                              "test_index"
                                              {:match_all {}}
                                              avg-aggs
                                              {:limit 5})

        stats-aggs {:price_stats {:stats {:field :price}}}
        {data-aggs-2 :data
         aggs-2 :aggs} (sut/query conn
                                     "test_index"
                                     {:match_all {}}
                                     stats-aggs
                                     {:limit 0})
        stats-aggs {:price_stats {:stats {:field :price}}}
        {data-aggs-3 :data
         aggs-3 :aggs} (sut/query conn
                                     "test_index"
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
      (is (= (repeat 3 "test_index")
             (->> (:data ids-query-result-2)
                  (map :_index))))
      (is (not-any? is-full-hits? (:data ids-query-result-1))
          "by default, full-hits? is set to false"))
    (testing "track_total_hits should be properly considered"
      (is (= 2 (-> (sut/query conn
                                 "test_index"
                                 (query/ids sample-3-ids)
                                 {:track_total_hits 2})
                   :paging
                   :total-hits)))
      (is (= 3
             (-> (sut/query conn
                               "test_index"
                               (query/ids sample-3-ids)
                               {:track_total_hits true})
                 :paging
                 :total-hits)
             (-> (sut/query conn
                               "test_index"
                               (query/ids sample-3-ids)
                               {})
                 :paging
                 :total-hits)))
      (is (= 0 (-> (sut/query conn
                                 "test_index"
                                 (query/ids sample-3-ids)
                                 {:track_total_hits false})
                   :paging
                   :total-hits))))
    (testing "sort and search_after params should be properly applied"
      (is (= '(6 7)
             (map :price (:data search_after-result))))
      (is (= [7]
             (-> search_after-result :paging :sort)
             (-> search_after-result :paging :next :search_after))))

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
             (:price_stats aggs-3))))
    ;; clean
    (es-index/delete! conn "test_index")))

(deftest ^:integration delete-by-query-test
  (let [sample-docs-1 (mapv #(assoc {:_index "test_index-1"
                                     :foo (if (< % 5)
                                            :bar1
                                            :bar2)}
                                    :_id %)
                            (range 10))
        sample-docs-2 (mapv #(assoc {:_index "test_index-2"
                                     :foo (if (< % 5)
                                            :bar1
                                            :bar2)}
                                    :_id %)
                            (range 10))
        conn (es-conn/connect {:host "localhost" :port 9200})
        q-term (query/term :foo :bar2)
        q-ids-1 (query/ids ["0" "1" "2"])
        q-ids-2 (query/ids ["3" "4"])]
    (es-index/delete! conn "test_index")
    (es-index/create! conn "test_index" {})
    (sut/bulk-create-doc conn sample-docs-1 {:refresh "true"})
    (sut/bulk-create-doc conn sample-docs-2 {:refresh "true"})
    (is (= 5
           (:deleted (sut/delete-by-query conn
                                          ["test_index-1"]
                                          q-term
                                          {:wait_for_completion true
                                           :refresh "true"})))
        "delete-by-query should delete all documents that match a query for given index")
    (is (= 6
           (:deleted (sut/delete-by-query conn
                                          ["test_index-1", "test_index-2"]
                                          q-ids-1
                                          {:wait_for_completion true
                                           :refresh "true"})))
        "delete-by-query should properly apply deletion on all given indices")
    (is (seq (:task (sut/delete-by-query conn
                                         ["test_index-1", "test_index-2"]
                                         q-ids-2
                                         {:wait_for_completion false
                                          :refresh "true"})))
        "delete-by-query with wait-for-completion? set to false should directly return an answer before deletion with a task id")
    (es-index/delete! conn "test_index")))


(deftest query-params-test
  (let [conn (es-conn/connect {:host "localhost" :port 9200})
        query-params (atom nil)
        check-query-params (fn [expected msg]
                             (is (= @query-params expected)
                                 msg)
                             (reset! query-params nil))]
    (with-fake-routes-in-isolation
      {#"http://localhost:9200/.*"
       (fn [{:keys [query-string]}]
         (reset! query-params
                 (keywordize-keys (form-decode query-string)))
         {:status 200
          :headers {:content-type "application/clojure"}})}

      (testing "update query parameter"
        (sut/update-doc conn "test_index" "1" {:foo "bar"} {})
        (check-query-params {:_source "true"
                             :retry_on_conflict "5"}
                            "default opts should be properly set in update-doc")
        (sut/update-doc conn
                        "test_index"
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
        (sut/get-doc conn "test_index" "1" {})
        (check-query-params ""
                            "no default query-params")
        (sut/get-doc conn "test_index" "1" {:_source ["foo" "bar"]
                                            :to-be-ignored "ignored"})
        (check-query-params {:_source ["foo" "bar"]}
                            "only `_source` is accepted as query-parmas"))
      (testing "create-doc query parameters"
        (sut/create-doc conn "test_index" {:a "doc"} {})
        (check-query-params {:op_type "create"}
                            "create-doc should set `op_type` to force error when document exists")
        (sut/create-doc conn "test_index" {:a "doc"} {:refresh "wait_for"
                                                      :to-be-ignored "ignored"})
        (check-query-params {:op_type "create"
                             :refresh "wait_for"}
                            "`refresh` is the only accepted query parameter"))
      (testing "index-doc query parameters"
        (sut/index-doc conn "test_index" {:a "doc"} {})
        (check-query-params "" "index-doc has no default query parameter")
        (sut/index-doc conn
                       "test_index"
                       {:a "doc"}
                       {:op_type "index"
                        :refresh "true"
                        :to-be-ignored "ignored"})
        (check-query-params {:op_type "index"
                             :refresh "true"}
                            "accepted query parameter are `refresh` and `op_type"))
      (testing "delete-doc query parameters"
        (sut/delete-doc conn "test_index" "1" {})
        (check-query-params "" "delete-doc has no default query parameter")
        (sut/delete-doc conn "test_index" "1" {:refresh "wait_for"
                                               :to-be-ignored "ignored"})
        (check-query-params {:refresh "wait_for"}
                            "`refresh` is the only accepted query parameter")))))
