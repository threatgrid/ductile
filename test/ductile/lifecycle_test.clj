(ns ductile.lifecycle-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [ductile.lifecycle :as sut]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :once validate-schemas)

(def sample-ilm-policy
  "Sample ILM policy with multiple phases"
  {:phases
   {:hot {:min_age "0ms"
          :actions {:rollover {:max_docs 100000000
                               :max_age "7d"}}}
    :warm {:min_age "7d"
           :actions {:readonly {}
                     :force_merge {:max_num_segments 1}}}
    :delete {:min_age "30d"
             :actions {:delete {}}}}})

(def sample-ilm-simple
  "Simple ILM policy with just rollover and delete"
  {:phases
   {:hot {:actions {:rollover {:max_docs 100000}}}
    :delete {:min_age "30d"
             :actions {:delete {}}}}})

(deftest transform-ilm-to-ism-simple-test
  (testing "Transform simple ILM policy to ISM"
    (let [result (sut/transform-ilm-to-ism sample-ilm-simple)]
      ;; Check structure
      (is (contains? result :states))
      (is (contains? result :default_state))
      (is (= 1 (:schema_version result)))

      ;; Check states
      (let [states (:states result)]
        (is (= 2 (count states)))

        ;; Check hot state
        (let [hot-state (first states)]
          (is (= "hot" (:name hot-state)))
          (is (vector? (:actions hot-state)))
          (is (= 1 (count (:actions hot-state))))
          (let [action (first (:actions hot-state))]
            (is (contains? action :rollover))
            (is (= 100000 (get-in action [:rollover :min_doc_count]))))
          ;; Check transition to delete
          (is (= "delete" (get-in hot-state [:transitions 0 :state_name])))
          (is (= "30d" (get-in hot-state [:transitions 0 :conditions :min_index_age]))))

        ;; Check delete state
        (let [delete-state (second states)]
          (is (= "delete" (:name delete-state)))
          (is (= 1 (count (:actions delete-state))))
          (is (contains? (first (:actions delete-state)) :delete))
          (is (nil? (:transitions delete-state))))))))

(deftest transform-ilm-to-ism-complex-test
  (testing "Transform complex ILM policy with multiple phases"
    (let [result (sut/transform-ilm-to-ism sample-ilm-policy)]
      (let [states (:states result)]
        (is (= 3 (count states)))

        ;; Check hot state
        (let [hot-state (first states)]
          (is (= "hot" (:name hot-state)))
          (let [action (first (:actions hot-state))]
            (is (contains? action :rollover))
            (is (= "7d" (get-in action [:rollover :min_index_age])))
            (is (= 100000000 (get-in action [:rollover :min_doc_count])))))

        ;; Check warm state
        (let [warm-state (second states)]
          (is (= "warm" (:name warm-state)))
          (is (= 2 (count (:actions warm-state))))
          ;; Should have readonly and force_merge actions
          (is (some #(contains? % :read_only) (:actions warm-state)))
          (is (some #(contains? % :force_merge) (:actions warm-state))))

        ;; Check delete state
        (let [delete-state (nth states 2)]
          (is (= "delete" (:name delete-state)))
          (is (contains? (first (:actions delete-state)) :delete)))))))

(deftest transform-ilm-action-types-test
  (testing "Transform various ILM action types"
    (testing "Rollover action"
      (let [policy {:phases {:hot {:actions {:rollover {:max_docs 1000
                                                         :max_size "50gb"
                                                         :max_age "7d"}}}}}
            result (sut/transform-ilm-to-ism policy)
            hot-action (first (get-in result [:states 0 :actions]))]
        (is (= 1000 (get-in hot-action [:rollover :min_doc_count])))
        (is (= "50gb" (get-in hot-action [:rollover :min_size])))
        (is (= "7d" (get-in hot-action [:rollover :min_index_age])))))

    (testing "Delete action"
      (let [policy {:phases {:delete {:actions {:delete {}}}}}
            result (sut/transform-ilm-to-ism policy)
            delete-action (first (get-in result [:states 0 :actions]))]
        (is (contains? delete-action :delete))))

    (testing "Force merge action"
      (let [policy {:phases {:warm {:actions {:force_merge {:max_num_segments 1}}}}}
            result (sut/transform-ilm-to-ism policy)
            action (first (get-in result [:states 0 :actions]))]
        (is (= 1 (get-in action [:force_merge :max_num_segments])))))))

(deftest transform-ism-to-ilm-test
  (testing "Transform ISM policy back to ILM"
    (let [ism-policy {:states [{:name "hot"
                                :actions [{:rollover {:min_doc_count 100000
                                                      :min_index_age "7d"}}]
                                :transitions [{:state_name "delete"
                                               :conditions {:min_index_age "30d"}}]}
                               {:name "delete"
                                :actions [{:delete {}}]}]
                      :default_state "hot"
                      :schema_version 1}
          result (sut/transform-ism-to-ilm ism-policy)]
      ;; Check phases
      (is (contains? result :phases))
      (is (contains? (:phases result) :hot))
      (is (contains? (:phases result) :delete))

      ;; Check hot phase
      (let [hot-phase (get-in result [:phases :hot])]
        (is (contains? (:actions hot-phase) :rollover))
        (is (= 100000 (get-in hot-phase [:actions :rollover :max_docs])))
        (is (= "7d" (get-in hot-phase [:actions :rollover :max_age])))
        (is (= "30d" (:min_age hot-phase))))

      ;; Check delete phase
      (let [delete-phase (get-in result [:phases :delete])]
        (is (contains? (:actions delete-phase) :delete))))))

(deftest round-trip-transformation-test
  (testing "ILM -> ISM -> ILM should preserve core policy structure"
    (let [original sample-ilm-simple
          ism (sut/transform-ilm-to-ism original)
          back-to-ilm (sut/transform-ism-to-ilm ism)]
      ;; Check that we get back the essential structure
      (is (contains? back-to-ilm :phases))
      (is (contains? (:phases back-to-ilm) :hot))
      (is (contains? (:phases back-to-ilm) :delete))

      ;; Check hot phase rollover
      (is (= 100000 (get-in back-to-ilm [:phases :hot :actions :rollover :max_docs])))

      ;; Check delete phase
      (is (contains? (get-in back-to-ilm [:phases :delete :actions]) :delete)))))

(deftest normalize-policy-test
  (testing "normalize-policy for Elasticsearch keeps ILM format"
    (let [result (sut/normalize-policy sample-ilm-simple :elasticsearch)]
      (is (contains? result :phases))
      (is (= sample-ilm-simple result))))

  (testing "normalize-policy for OpenSearch transforms to ISM"
    (let [result (sut/normalize-policy sample-ilm-simple :opensearch)]
      (is (contains? result :states))
      (is (not (contains? result :phases)))))

  (testing "normalize-policy with ISM policy for OpenSearch returns as-is"
    (let [ism-policy {:states [{:name "hot" :actions []}]
                      :default_state "hot"}
          result (sut/normalize-policy ism-policy :opensearch)]
      (is (= ism-policy result))))

  (testing "normalize-policy with ISM policy for Elasticsearch transforms to ILM"
    (let [ism-policy {:states [{:name "hot"
                                :actions [{:rollover {:min_doc_count 1000}}]}
                               {:name "delete"
                                :actions [{:delete {}}]}]
                      :default_state "hot"}
          result (sut/normalize-policy ism-policy :elasticsearch)]
      (is (contains? result :phases))
      (is (not (contains? result :states))))))

(deftest phase-ordering-test
  (testing "ISM states are created in correct phase order"
    (let [policy {:phases {:delete {:min_age "30d" :actions {:delete {}}}
                           :warm {:min_age "7d" :actions {:readonly {}}}
                           :hot {:actions {:rollover {:max_docs 1000}}}}}
          result (sut/transform-ilm-to-ism policy)
          state-names (map :name (:states result))]
      ;; Should be ordered: hot, warm, delete
      (is (= ["hot" "warm" "delete"] state-names)))))

(deftest min-age-parsing-test
  (testing "Parse various min_age formats"
    (let [policy-7d {:phases {:hot {:actions {:rollover {}}}
                              :delete {:min_age "7d" :actions {:delete {}}}}}
          result-7d (sut/transform-ilm-to-ism policy-7d)]
      (is (= "7d" (get-in result-7d [:states 0 :transitions 0 :conditions :min_index_age]))))

    (let [policy-30d {:phases {:hot {:actions {:rollover {}}}
                               :delete {:min_age "30d" :actions {:delete {}}}}}
          result-30d (sut/transform-ilm-to-ism policy-30d)]
      (is (= "30d" (get-in result-30d [:states 0 :transitions 0 :conditions :min_index_age]))))))
