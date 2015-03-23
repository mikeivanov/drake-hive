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

(deftest test-hive-parse-metadata
  (let [meta [{:comment nil
               :col_name "Database:"
               :data_type "default"}
              {:comment nil
               :data_type "Tue Apr 22 07:21:31 PDT 2014"
               :col_name "CreateTime:         "}
              {:comment nil
               :data_type "MANAGED_TABLE       "
               :col_name "Table Type:         "}
              {:comment "1398176493          "
               :data_type "transient_lastDdlTime"
               :col_name ""}
              {:comment nil
               :data_type "No"
               :col_name "Compressed:"}
              ]]
    (let [[ctime mtime] (h/parse-metadata meta)]
      (is (= 1398176491000 ctime))
      (is (= 1398176493000 mtime)))))

