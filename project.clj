(def clj-version "1.10.1")

(defproject threatgrid/ductile "0.1.0-SNAPSHOT"
  :description "Yet another Clojure client for Elasticsearch REST API, that fits our needs"
  :url "https://github.com/threatgrid/ductile"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure ~clj-version]
                 [org.clojure/tools.logging "0.5.0"]
                 [org.clojure/tools.namespace "1.0.0"]
                 [prismatic/schema "1.1.12"]
                 [metosin/schema-tools "0.12.2"]
                 [clj-http "3.10.1"]
                 [com.arohner/uri "0.1.2"]
                 [cheshire "5.9.0"]]
  :repositories [["sonatype-snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"}]]
  :main nil
  :codox {:output-path "doc"
          :source-paths ["src"]}
  :plugins [[lein-codox "0.10.7"]
            [lein-pprint "1.3.2"]]
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all
                       :pedantic? :abort}
             :dev {:dependencies
                   [[ch.qos.logback/logback-classic "1.2.3"]]
                   :pedantic? :warn}
             :test {:dependencies
                    [[clj-http-fake "1.0.3"]
                     [ring/ring-codec "1.1.2"]]
                    :resource-paths ["test/resources"]
                    :pedantic? :abort}}
  :global-vars {*warn-on-reflection* true})
