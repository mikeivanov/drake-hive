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
  (let [{:keys [datasource database table]} path]
    (boolean (first (list-tables datasource database table)))))

(defn table-mod-time [path]
  (let [{:keys [datasource database table]} path
        props   (query datasource
                       (format "show tblproperties `%s.%s`" database table))
        [_ val] (->> props
                     (drop-while (fn [[prop _]] (not= prop "transient_lastDdlTime")))
                     (first))]
    (* 1000 (Long/parseLong val))))

(defn table-info [table-path]
  (let [path (hive-path table-path)]
    (when (table-exists? path)
      {:mod-time (table-mod-time path)
       :path (:normalized path)
       :directory false})))

(defn hive []
  (reify FileSystem
    (exists? [_ path]
      (table-exists? (hive-path path)))
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
      (table-exists? (hive-path path)))
    (normalized-filename [_ path]
      (:normalized (hive-path path)))
    (rm [_ _]
      (throw+ {:msg "rm is not implemented on hive filesystem"}))
    (mv [_ _ _]
      (throw+ {:msg "mv is not implemented on hive filesystem"}))))
