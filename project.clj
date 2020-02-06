(def clj-version "1.10.1")
(def metrics-clojure-version "2.10.0")

(defproject ductile "0.1.0-SNAPSHOT"
  :description "Yet another Clojure client for Elasticsearch REST API, that fits our needs"
  :url "https://github.com/threatgrid/ductile"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure ~clj-version]
                 [org.clojure/tools.logging "0.5.0"]
                 [prismatic/schema "1.1.12"]
                 [metosin/schema-tools "0.12.2"]
                 [clj-http "3.10.0"] ;TODO bump clj-http with https://github.com/dakrone/clj-http/pull/532
                 [com.arohner/uri "0.1.2"]
                 [cheshire "5.9.0"]
                 [riemann-clojure-client "0.5.1"]
                 [metrics-clojure ~metrics-clojure-version]
                 [metrics-clojure-jvm ~metrics-clojure-version]
                 [metrics-clojure-ring ~metrics-clojure-version]
                 [metrics-clojure-riemann ~metrics-clojure-version
                  :exclusions [;renamed to (and incompatible with) `io.riemann/riemann-java-client`,
                                        ;which is provided by `riemann-clojure-client`.
                               com.aphyr/riemann-java-client]]
                 [clout "2.2.1"]]
  :main nil
  :codox {:output-path "doc"
          :source-paths ["src"]}
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :dev {:dependencies
                   [[ch.qos.logback/logback-classic "1.2.3"]]
                   :resource-paths ["test/resources"]}}
  :global-vars {*warn-on-reflection* true})
