(ns ductile.pagination
  (:require [schema.core :as s]))

(def default-limit 100)

;; https://www.elastic.co/guide/en/elasticsearch/reference/current/index-modules.html#dynamic-index-settings
(def max-result-window 10000)

(defn list-response-schema
  "generate a list response schema for a model"
  [Model]
  {:data [Model]
   :paging {s/Any s/Any}})

(defn response
  "Make a paginated response adding pagination helpers into a map"
  [results
   {:keys [offset
           limit
           sort
           search_after
           total-hits]}]
  (let [offset (or offset 0)
        limit (or limit default-limit)
        previous-offset (- offset limit)
        next-offset (+ offset limit)
        previous? (and (not search_after)
                       (pos? offset)
                       (> max-result-window (+ offset limit)))
        next? (if search_after
                (= limit (count results))
                (> total-hits next-offset))
        previous {:previous {:limit limit
                             :offset (if (> previous-offset 0)
                                       previous-offset 0)}}
        next {:next
              (cond->
                  {:limit limit
                   :offset next-offset}
                sort (assoc :search_after sort))}]
    {:data results
     :paging (merge
              {:total-hits total-hits}
              (when previous? previous)
              (when next? next)
              (when sort {:sort sort}))}))

(defn paginate
  [data {:keys [sort_by sort_order offset limit]
         :or {sort_by :id
              sort_order :asc
              offset 0
              limit default-limit}}]
  (as-> data $
    (sort-by sort_by $)
    (if (= :desc sort_order)
      (reverse $) $)
    (drop offset $)
    (take limit $)))
