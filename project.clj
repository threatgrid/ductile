(defproject threatgrid/ductile "0.4.9-SNAPSHOT"
  :description "Yet another Clojure client for Elasticsearch REST API, that fits our needs"
  :url "https://github.com/threatgrid/ductile"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.2.4"]
                 [prismatic/schema "1.4.1"]
                 [metosin/schema-tools "0.12.3"]
                 [clj-http "3.12.3"]
                 [cheshire "5.11.0"]]
  ;; make `lein deploy` use clojars by default
  :deploy-repositories [["releases" :clojars]
                        ["snapshots" :clojars]]
  :repositories [["sonatype-snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots/"}]]
  :codox {:output-path "doc"
          :source-paths ["src"]}
  :plugins [[lein-codox "0.10.7"]
            [lein-pprint "1.3.2"]]
  :target-path "target/%s"
  :test-selectors {:default (every-pred (complement :integration) (complement :encoding))
                   :integration :integration
                   :all (complement :encoding)}
  :profiles {:uberjar {:aot :all
                       :pedantic? :abort}
             :dev {:dependencies [[ch.qos.logback/logback-classic "1.2.3"]]
                   :pedantic? :warn}
             :test {:dependencies [[ring/ring-codec "1.1.2"]]
                    :resource-paths ["test/resources"]
                    :pedantic? :warn}
             :test-encoding {:jvm-opts ["-Dfile.encoding=ANSI_X3.4-1968"]
                             :test-selectors ^:replace {:default :encoding}}}
  :global-vars {*warn-on-reflection* true})
