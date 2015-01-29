(defproject mikeivanov/drake-hive "0.1.1-SNAPSHOT"
  :description "A Hive plugin for Drake."
  :url "https://github.com/mikeivanov/drake-hive"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [factual/drake-interface "0.0.1"]
                 [org.clojure/java.jdbc "0.3.6"]
                 [org.apache.hive/hive-jdbc "0.13.1"]]
  :profiles {:dev {:dependencies [[factual/drake "0.1.6"]]}}
  :plugins [[cider/cider-nrepl "0.8.1"]]
  :aliases {"cider" ["repl" ":headless"]})
