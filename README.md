# ductile

A minimalist clojure library for Elasticsearch REST API.

It's currently compatible with Elasticsearch 7.X

## Usage


* Create a connection to an elasticsearch instance
```clojure
(require '[ductile.conn :as es-conn])

(def c (es-conn/connect {:host "localhost" :port 9200}))
```

* index operations
```clojure
(require '[ductile.index :as es-index])

(sut/index-exists? conn "new_index")
;;false

(def test-config {:settings {:number_of_shards 3
                             :number_of_replicas 1
                             :refresh_interval "1s"}
                  :mappings {:properties {:name {:type :text}
                                          :age {:type :long}
                                          :description {:type :text}}}
                  :aliases {:test-alias {}}})

(es-index/create! c "test-index" test-config)

;; you can then delete or close that index
(es-index/close! c "test-index")
(es-index/delete! c "test-index")


;; you can also manage templates
(es-index/create-template! c "test-index" test-config ["foo*" "bar*"])

;; when the index-patterns are not provided, one will be generated from the name with a wildcard suffix
;; for instance, the following template will have the index-patterns ["test-index*"]
(es-index/create-template! c "test-index" test-config)

(es-index/get-template c "test-index")
(es-index/delete-template! c "test-index")
```

* crud operations
  * create a document, and use the id field as document id
  ``` clojure
  (require '[ductile.document :as es-doc]) 
  (es-doc/create-doc c
                     "test-index"
                     {:id 1
                     :name "John Doe"
                     :description "an anonymous coward"}
                     :wait_for)
  ```
  ``` javascript
  {:id 1, :name "John Doe", :description "an anonymous coward"}
  ```

  if you try to create another document with the same id, it will throw an ExceptionInfo
  ``` clojure
  (es-doc/create-doc c
                        "test-index"
                     {:id 1
                      :name "Jane Doe"
                      :description "another anonymous coward"}
                     :wait_for)
  ;; Execution error (ExceptionInfo) at ductile.conn/safe-es-read (conn.clj:54).
  ;; ES query failed
  ```
  it will return created document
  ``` javascript
  {:id "1", :name "Jane Doe", :description "another anonymous coward"}
  ```
  
  ;; if you do not provide the id field, elasticsearch will insert the document and generate an id
  ``` clojure
  (es-doc/create-doc c
                     "test-index"
                     {:name "Jane Doe 2"
                      :description "yet another anonymous coward"}
                     :wait_for)
  ```
  ``` javascript
  {:name "Jane Doe 2", :description "yet another anonymous coward", :id "EBD9L3ABLWPPOW84CV6I"}
  ```
  
  you can similarly create a document with index-doc, but if the document already exists it will erase it
  ``` clojure
  (es-doc/index-doc c
                    "test-index"
                    {:id 2
                     :name "Jane Doe"
                     :description "another anonymous coward"}
                    :wait_for)
  
  (es-doc/index-doc c
                    "test-index"
                    {:name "John Doe"
                     :description "not so anonymous coward"}
                    :wait_for)
  ```                  
  the 4th parameter offers to set the `refresh` parameter and can take same string values as corresponding ES query parameter: `true`, 'false', 'wait_for'

  * patch a document
  ```clojure
  (es-doc/update-doc c
                     "test-index"
                     1
                     {:age 36
                      :description "anonymous but known age"}
                     :wait_for)
  ```
  ```javascript
  {:id 1, :name "Jane Doe", :description "anonymous with know age", :age 36}                   
   ```                  
  * retrieve a document 
  ```clojure
  (es-doc/get-doc c
                  "test-index"
                  1
                  {})
     ```
     ``` javascript
  {:id 1, :name "Jane Doe", :description "anonymous with know age", :age 36}
     ```
     
   * delete a document
  ``` clojure
  (es-doc/delete-doc c
                    "test-index"
                    1
                    :wait_for)
   ;; true
   ```

* and of course you can query it!
you can either provide classical elasticsearch queries or use some helpers from `ductile.query` namespace
```clojure
(require `[ductile.query :as es-query])
(es-doc/query c
              "test-index"
              (es-query/ids [1 2])
              {})
```
``` javascript
{:data
 ({:id 2, :name "Jane Doe", :description "another anonymous coward"}),
 :paging {:total-hits 1}}
```
if you need all metadata you can use the full-hits? option
``` clojure
(clojure.pprint/pprint
  (es-doc/query c
                "test-index"
                {:match_all {}}
                {:full-hits? true
                 :sort {"id" {:order :desc}}
                 :limit 2}))
```
it will return not only the matched documents but also meta data like `_index` and `_score`
``` javascript
{:data
 [{:_index "test-index",
   :_type "_doc",
   :_id "2",
   :_score nil,
   :_source
   {:id 2, :name "Jane Doe", :description "another anonymous coward"},
   :sort [2]}
  {:_index "test-index",
   :_type "_doc",
   :_id "1",
   :_score nil,
   :_source
   {:id 1, :name "Jane Doe", :description "another anonymous coward"},
   :sort [1]}],
 :paging
 {:total-hits 3,
  :next {:limit 2, :offset 2, :search_after [1]},
  :sort [1]}}
```
Ductile also provides a search function with a simple interface that offers to use a Mongo like filters lucene query string to easily match documents
``` clojure
(es-doc/search-docs c
                    "test-index"
                    {:query_string {:query "anonymous"}}
                    {:age 36}
                    {:sort {:name {:order :desc}}})
```


## License

Copyright © Cisco Systems

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.
