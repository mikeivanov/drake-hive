(ns drake-hive.integration-test
  (:require [drake.fs :as fs]
            [drake-interface.core :as di]
            [drake.fs.hive :as h]
            [clojure.test :refer [deftest is use-fixtures]]
            [slingshot.slingshot :refer [throw+]]
            [clojure.java.shell :refer [sh]]))

(defn cmd [path & args]
  (let [res (apply sh path args)]
    (when (not= 0 (:exit res))
      (throw+ {:msg (format "Command error: %s" (:err res))}))
    (:out res)))

(defn hive-sandbox-fixture [body]
  (try
    (cmd "hive" "-e" (str "drop database if exists hive_drake_test cascade;"
                          "create database hive_drake_test;"))
    (body)
    (finally
      (cmd "hive" "-e" (str "drop database if exists hive_drake_test cascade;")))))

(use-fixtures :once hive-sandbox-fixture)

(deftest test-hive-fs
  (cmd "hive" "-e" "use hive_drake_test; create table foo (bar int)")
  (is (= "hive://127.0.0.1:10000/hive_drake_test/foo"
         (fs/normalized-path "hive:/hive_drake_test/foo")))
  (is (= "hive://localhost:10000/hive_drake_test/foo"
         (fs/normalized-path "hive://localhost:10000/hive_drake_test/foo")))
  (is (fs/fs di/exists? "hive:/hive_drake_test/foo")))
