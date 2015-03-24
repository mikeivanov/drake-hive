(ns drake-hive.fs-test
  (:require [drake.fs.hive :as h]
            [clojure.test :refer [deftest is]]
            [slingshot.slingshot :refer [throw+]]))

(deftest test-hive-path-uri
  (let [path (h/hive-path "hive://urihost:7890/database/table")]
    (is (= "database" (:database path)))
    (is (= "table" (:table path)))
    (is (= "//urihost:7890/database/table" (:normalized path)))
    (is (= "//urihost:7890" (:subname (:datasource path))))))

(deftest test-hive-path-env
  (binding [h/getenv (fn [] {"DRAKE_HIVE_HOST" "envhost"
                             "DRAKE_HIVE_PORT" "3456"
                             "DRAKE_HIVE_USER" "envuser"
                             "DRAKE_HIVE_PASSWORD" "envpassword"})]
    (let [path (h/hive-path "hive:/database/table")]
      (is (= "database" (:database path)))
      (is (= "table" (:table path)))
      (is (= "//envhost:3456/database/table" (:normalized path)))
      (is (= "//envhost:3456" (:subname (:datasource path))))
      (is (= "envuser" (:user (:datasource path))))
      (is (= "envpassword" (:password (:datasource path)))))))

