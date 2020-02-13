(ns ductile.query-test
  (:require [ductile.query :as sut]
            [clojure.test :refer [deftest is]]))


(deftest prepare-terms
  (let [simple-keys {:a 1 :b [2 3]}
        nested-keys {[:a :b] 1
                     [:c :d :e] [2 3]}]
    (is (= [{:terms {"a" '(1)}}
           {:terms {"b" '(2 3)}}]
           (sut/prepare-terms simple-keys)))
    (is (= [{:terms {"a.b" '(1)}}
           {:terms {"c.d.e" '(2 3)}}]
           (sut/prepare-terms nested-keys)))))


(deftest filter-map->terms-query-test
  (let [all-of {:a 1 :b [2 3]}
        query {:match {:title "this is a test"}}

        all-of-filters [{:terms {"a" '(1)}}
                        {:terms {"b" '(2 3)}}]

        bool-query1 (sut/filter-map->terms-query all-of query)
        bool-query2 (sut/filter-map->terms-query all-of)
        bool-query3 (sut/filter-map->terms-query nil nil)]


    (is (= (sut/bool {:filter (conj all-of-filters query)})
           bool-query1)
        "filter-map->bool-query with non empty params should return a bool query with formatted all-of elements and query in filter clause")

    (is (= (sut/bool {:filter all-of-filters})
           bool-query2)
        "filter-map->bool-query with only all-of param should return a bool query with only formatted all-of elements in filter clause")

  (is (= (sut/bool {:filter [{:match_all {}}]})
         bool-query3)
      "filter-map->bool-query with every empty params should return a bool query with only formatted all-of elements in filter clause")))

(deftest ids-test
  (is (= {:ids {:values ["id1" "id2" "id3"]}}
         (sut/ids ["id1" "id2" "id3"])))
  (is (= {:ids {:values ["id1"]}}
         (sut/ids ["id1"])))
  (is (= {:ids {:values []}}
         (sut/ids []))))
