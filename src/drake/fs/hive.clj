(ns drake.fs.hive
  (:require [drake-interface.core :refer [FileSystem]]
            [drake.fs :refer [remove-extra-slashes]]
            [slingshot.slingshot :refer [throw+]]
            [clojure.string :as s]
            [clojure.java.jdbc :as jdbc])
  (:import [java.net URI]))

(defn datasource [host port & [user password]]
  (let [user (or user
                 (get (System/getenv) "DRAKE_HIVE_USER")
                 (get (System/getenv) "USER")
                 "hive")
        password (or password
                     (get (System/getenv) "DRAKE_HIVE_PASSWORD")
                     "")]
    {:classname "org.apache.hive.jdbc.HiveDriver"
     :subprotocol "hive2"
     :subname (format "//%s:%d" host port)
     :user user
     :password password}))

(defn hive-path [path]
  (let [uri (URI. path)
        host (or (.getHost uri) "127.0.0.1")
        port (.getPort uri)
        port (if (pos? port) port 10000)
        [_ database table] (-> (or (.getPath uri) "")
                               (remove-extra-slashes)
                               (s/split #"/"))]
    (cond (not database) (throw (Exception. (str "malformed hive uri (missing database): " path)))
          (not table) (throw (Exception. (str "malformed hive uri (missing table): " path)))
          :else {:path (format "hive://%s:%d/%s/%s" host port database table)
                 :host host
                 :port port
                 :database database
                 :table table
                 :datasource (datasource host port)})))

(defn query [ds sql & args]
  (rest (jdbc/query ds (cons sql args) :as-arrays? true)))

(defn list-tables [ds database & [table]]
  (let [database (or database "default")
        table (or table "%")
        connection (jdbc/get-connection ds)
        metadata (.getMetaData connection)
        tables (.getTables metadata nil database table nil)]
    (doall (for [_ (range) :while (.next tables)]
             (.getString tables 3)))))

(defn table-exists? [path]
  (let [{ds :datasource db :database table :table} (hive-path path)]
    (boolean (first (list-tables ds db table)))))

(defn table-info [path]
  (when (table-exists? path)
    (let [{ds :datasource db :database table :table} (hive-path path)
          sql (format "describe extended %s.%s" db table)
          [_ meta] (->> (jdbc/query ds [sql] :as-arrays? true)
                        (rest)
                        (filter #(= "Detailed Table Information" (first %)))
                        (first))
          [_ ctime] (re-find #"createTime:(\d+)\b" meta)
          [_ mtime] (re-find #"transient_lastDdlTime=(\d+)\b" meta)
          [_ numrw] (re-find #"numRows=(\d+)\b" meta)]
      {:mod-time (* 1000 (Long/parseLong (or mtime ctime)))
       :num-rows (and numrw (Long/parseLong numrw))
       :path path
       :directory false})))

(defn hive []
  (reify FileSystem
    (exists? [_ path]
      (table-exists? path))
    (directory? [_ path]
      false)
    (mod-time [_ path]
      (:mod-time (table-info path)))
    (file-seq [_ path]
      [path])
    (file-info [_ path]
      (table-info path))
    (file-info-seq [_ path]
      [(table-info path)])
    (data-in? [_ path]
      (table-exists? path))
    (normalized-filename [_ path]
      (:path (hive-path path)))
    (rm [_ _]
      (throw+ {:msg "rm is not implemented on hive filesystem"}))
    (mv [_ _ _]
      (throw+ {:msg "mv is not implemented on hive filesystem"}))))
