(ns drake.fs.hive
  (:require [drake-interface.core :refer [FileSystem]]
            [drake.fs :refer [remove-extra-slashes]]
            [slingshot.slingshot :refer [throw+]]
            [clojure.string :as s]
            [clojure.java.jdbc :as jdbc])
  (:import [java.net URI]
           [java.util Date]
           [java.text SimpleDateFormat]))

(def ^:dynamic getenv (fn [] (System/getenv)))

(defn datasource [host port user password]
  {:classname "org.apache.hive.jdbc.HiveDriver"
   :subprotocol "hive2"
   :subname (format "//%s:%s" host port)
   :user user
   :password password})

(defn hive-path [path]
  (let [env (getenv)
        uri (URI. path)
        host (or (.getHost uri) (get env "DRAKE_HIVE_HOST" "127.0.0.1"))
        port (let [port (.getPort uri)]
               (if (= port -1)
                 (get env "DRAKE_HIVE_PORT" "10000")
                 port))
        [_ database table] (-> (or (.getPath uri) "")
                               (remove-extra-slashes)
                               (s/split #"/"))
        user (or (get env "DRAKE_HIVE_USER")
                 (get env "USER")
                 "hive")
        password (get env "DRAKE_HIVE_PASSWORD" "")]
    (when-not database
      (throw+ {:msg (str "malformed hive uri (missing database): " path)}))
    (when-not table
      (throw+ {:msg (str "malformed hive uri (missing table): " path)}))
    {:normalized (format "//%s:%s/%s/%s" host port database table)
     :database database
     :table table
     :datasource (datasource host port user password)}))

(defn query [ds sql & args]
  (rest (jdbc/query ds (cons sql args) :as-arrays? true)))

(defn list-tables [ds database & [table]]
  (let [table (or table "%")
        connection (jdbc/get-connection ds)
        metadata (.getMetaData connection)
        tables (.getTables metadata nil database table nil)]
    (doall (for [_ (range) :while (.next tables)]
             (.getString tables 3)))))

(defn table-exists? [path]
  (let [{:keys [datasource database table]} (hive-path path)]
    (boolean (first (list-tables datasource database table)))))

(defn parse-timestamp [v]
  (let [fmt (SimpleDateFormat. "EEE MMM d HH:mm:ss zzz yyyy")]
    (.getTime (.parse fmt v))))

(defn trim [s]
  (when s (s/trim s)))

(defn parse-metadata [meta]
  (loop [meta meta ctime nil mtime nil]
    (if-let [m (first meta)]
      ;; Hive, oh Hive...
      (let [name  (trim (:col_name m))
            type  (trim (:data_type m))
            comm  (trim (:comment m))
            ctime (if (and (= name "CreateTime:") type)
                    (parse-timestamp type) ; <- not a bug!
                    ctime)
            mtime (if (and (= type "transient_lastDdlTime") comm) ; <- not a bug!
                    (* 1000 (Long/parseLong comm)))] ; <- not a bug!
        (recur (rest meta) ctime mtime))
      [ctime mtime])))

(defn table-info [table-path]
  (when (table-exists? table-path)
    (let [{:keys [datasource database table normalized]} (hive-path table-path)
          ;; note: it has to be "describe formatted" because
          ;; "describe extended" omits the "transient_lastDdlTime" field.
          sql (format "describe formatted %s.%s" database table)
          meta (jdbc/query datasource [sql])
          [ctime mtime] (parse-metadata meta)]
      {:mod-time (or mtime ctime)
       :path normalized
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
      (:normalized (hive-path path)))
    (rm [_ _]
      (throw+ {:msg "rm is not implemented on hive filesystem"}))
    (mv [_ _ _]
      (throw+ {:msg "mv is not implemented on hive filesystem"}))))
