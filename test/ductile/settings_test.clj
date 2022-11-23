(ns ductile.settings-test
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [ductile.settings :as sut]
            [ductile.index :as index]
            [ductile.test-helpers :refer [for-each-es-version]]
            [schema.test :refer [validate-schemas]]))

(use-fixtures :once validate-schemas)

(deftest ^:integration fetch-test
  (let [indices (cons "settings-test-fetch"
                      (map #(str "settings-test-fetch-" %) (range 10)))]
    (for-each-es-version
     "fetch shall properly return settings"
     #(index/delete! conn "settings-test-*")

     (doseq [index indices]
       (index/create! conn index {}))

     (testing "fetch settings of specific index"
       (let [{:keys [settings-test-fetch]} (sut/fetch conn "settings-test-fetch")]
         (is (= 10000 (get-in settings-test-fetch [:index :max_result_window]))))
       (index/update-settings! conn "settings-test-fetch" {:max_result_window 20})
       (let [{:keys [settings-test-fetch]} (sut/fetch conn)]
         (is (= 20 (get-in settings-test-fetch [:index :max_result_window])))))

     (testing "fetch settings of all indices"
       (let [res (sut/fetch conn)]
         (is (= (set (map keyword indices))
                (set (keys res))))))))

  )
