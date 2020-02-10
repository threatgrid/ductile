# ductile

A minimalist clojure library for Elasticsearch REST API.

It's currently compatible with Elasticsearch 7.X

## Usage


* Create a connection to an elasticsearch instance
```clojure
(require '[ductile.conn :as es-conn])

(def c (es-conn/connect {:host "localhost" :port 9200}))
```

* crud operations
  * create a document, and use the id field as document id
  ``` clojure
  (require '[ductile.document :as es-doc]) 
  (es-doc/create-doc c
                     "test_index"
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
                        "test_index"
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
                     "test_index"
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
                    "test_index"
                    {:id 2
                     :name "Jane Doe"
                     :description "another anonymous coward"}
                    :wait_for)
  
  (es-doc/index-doc c
                    "test_index"
                    {:name "John Doe"
                     :description "not so anonymous coward"}
                    :wait_for)
  ```                  
  the 4th parameter offers to set the `refresh` parameter and can take same string values as corresponding ES query parameter: `true`, 'false', 'wait_for'

  * patch a document
  ```clojure
  (es-doc/update-doc c
                     "test_index"
                     1
                     {:age 36
                      :description "anonymous with know age"}
                     :wait_for)
  ```
  ```javascript
  {:id 1, :name "Jane Doe", :description "anonymous with know age", :age 36}                   
   ```                  
  * retrieve a document 
  ```clojure
  (es-doc/get-doc c
                  "test_index"
                  1
                  {})
     ```
     ``` javascript
  {:id 1, :name "Jane Doe", :description "anonymous with know age", :age 36}
     ```
     
   * delete a document
  ``` clojure
  (es-doc/delete-doc c
                    "test_index"
                    1
                    :wait_for)
   ;; true
   ```

* and of course you can query it!
you can either provide classical elasticsearch queries or use some helpers from `ductile.query` namespace
```clojure
(require `[ductile.query :as es-query])
(es-doc/query c
              "test_index"
              (es-query/ids [1 2])
              {})
```
``` javascript
{:data ({:id 2, :name "Jane Doe", :description "another anonymous coward"}), :paging {:total-hits 1}}
```
if you need all metadata you can use the full-hits? option
``` clojure
(clojure.pprint/pprint
  (es-doc/query c
                "test_index"
                {:match_all {}}
                {:full-hits? true
                 :sort {"id" {:order :desc}}
                 :limit 2}))
```
``` javascript
{:data
 [{:_index "test_index",
   :_type "_doc",
   :_id "2",
   :_score nil,
   :_source
   {:id 2, :name "Jane Doe", :description "another anonymous coward"},
   :sort [2]}
  {:_index "test_index",
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


## License

Copyright © Cisco Systems

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.
