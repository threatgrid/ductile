(ns ductile.query
  (:require [clojure.string :as str]
            [schema.core :as s]
            [ductile.schemas :refer [IdsQuery BoolQuery BoolQueryParams]]))

(s/defn ids :- IdsQuery
  "Ids Query"
  [ids :- [s/Str]]
  {:ids {:values ids}})

(s/defn bool :- BoolQuery
  "Boolean Query"
  [opts :- BoolQueryParams]
  {:bool opts})

(defn filtered
  "Filtered query"
  [opts]
  {:filtered opts})

(defn nested
  "Nested document query"
  [opts]
  {:nested opts})

(defn term
  "Term Query"
  ([key values] (term key values nil))
  ([key values opts]
   (merge { (if (coll? values) :terms :term) (hash-map key values) }
          opts)))

(defn terms
  "Terms Query"
  ([key values] (terms key values nil))
  ([key values opts]
   (term key values opts)))

(defn nested-terms
  "make nested terms from a filter:
  [[[:observable :type] ip] [[:observable :value] 42.42.42.1]]
  ->
  [{:terms {observable.type [ip]}} {:terms {observable.value [42.42.42.1]}}]

we force all values to lowercase, since our indexing does the same for all terms."
  [filters]
  (mapv (fn [[k v]]
          (terms (->> k
                      (map name)
                      (str/join "."))
                 (map #(if (string? %)
                         (str/lower-case %)
                         %)
                      (if (coll? v) v [v]))))
        filters))

(defn prepare-terms [filter-map]
  (let [terms (map (fn [[k v]]
                     (let [t-key (if (sequential? k) k [k])]
                       [t-key v]))
                   filter-map)]
    (nested-terms terms)))

(defn filter-map->terms-query
  "transforms a filter map to en ES terms query"
  ([filter-map]
   (filter-map->terms-query filter-map nil))
  ([filter-map query]
   (let [filter-terms (prepare-terms filter-map)]
     (bool {:filter
            (cond
              (every? empty? [query filter-map]) [{:match_all {}}]
              (empty? query) filter-terms
              :else (conj filter-terms query))}))))
