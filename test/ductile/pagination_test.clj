(ns ductile.pagination-test
  (:require [ductile.pagination :as sut]
            [clojure.test :refer [deftest is testing]]))

(deftest response-test
  (let [results (map #(assoc {} :id %)
                     (range 10))]
    (is (= {:data results
            :paging {:total-hits 100}}
           (sut/response results
                         {:total-hits 100}))
        "total-hits must be returned in the response")
    (is (= {:data results
            :paging {:total-hits 100
                     :sort [5 "value2"]}}
            (sut/response results
                          {:total-hits 100
                           :sort [5 "value2"]}))
        "sort values must be passed as result")
    (testing "search_after, next and previous fields should be properly for paginating"
      (is (= {:data results
              :paging {:total-hits 100
                       :previous {:limit 10
                                  :offset 70}
                       :next {:limit 10
                              :offset 90}}}
             (sut/response results
                           {:total-hits 100
                            :limit 10
                            :offset 80})))
      (is (= {:data results
              :paging {:total-hits 100
                       :previous {:limit 10
                                  :offset 80}}}
             (sut/response results
                           {:total-hits 100
                            :limit 10
                            :offset 90})))
      (is (= {:data results
              :paging {:total-hits 100
                       :next {:limit 10
                              :offset 10}}}
             (sut/response results
                           {:total-hits 100
                            :limit 10})))
      (is (= {:data results
              :paging {:total-hits 100
                       :next {:limit 10
                              :offset 20
                              :search_after [5 "value2"]}
                       :sort [5 "value2"]}}
             (sut/response results
                           {:total-hits 100
                            :limit 10
                            :offset 10
                            :sort [5 "value2"]
                            :search_after [4 "value1"]})))
      (is (= {:data (take 3 results)
              :paging {:total-hits 100}}
             (sut/response (take 3 results)
                           {:total-hits 100
                            :limit 10
                            :search_after [4 "value1"]}))))))
