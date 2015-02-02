(ns drake.protocol.hive
  (:require [drake-interface.core :refer [Protocol]]
            [drake.protocol :refer [create-cmd-file log-file]]
            [drake.fs :refer [get-fs]]
            [drake.fs.hive :refer [hive-path]]
            [fs.core :as fs]
            [drake.shell :refer [shell]]
            [slingshot.slingshot :refer [throw+]]
            [clojure.java.io :refer [writer]]))

(defn in-out-tables [vars]
  (into {} (for [[name value] vars
                 :let  [[match dir num] (re-find #"^(IN|OUT)PUT(|\d+)$" name)]
                 :when (and match (= "hive" (second (get-fs value))))]
             (let [{:keys [database table]} (hive-path value)]
               [(str "TABLE" dir num) (format "%s.%s" database table)]))))

(defn run-hive [{:keys [vars opts] :as step}]
  (let [output (vars "OUTPUT")
        outfs (second (get-fs output))
        capture (#{:capture "capture"} (:output opts))
        touch (#{:touch "touch"} (:output opts))
        logger (:logger opts "WARN")]
    (when (and capture (not= "file" outfs))
      (throw+ {:msg "output capture: not a local fs"}))
    (when (and touch (not= "file" outfs))
      (throw+ {:msg "output touch: not a local fs"}))
    (let [tables (in-out-tables vars)
          vars (merge vars tables)
          step (assoc step :vars vars)
          script-filename (create-cmd-file step)
          outputs (if capture
                    [(writer (fs/file output))]
                    [System/out (writer (log-file step "stdout"))])]
      (apply shell (concat ["hive"]
                           ["--hiveconf" (format "hive.root.logger=%s" logger) "-f"]
                           [script-filename
                            :env vars
                            :die true
                            :no-stdin (:no-stdin opts)
                            :out outputs
                            :err [System/err (writer (log-file step "stderr"))]]))
      (when touch
        (fs/touch output)))))

(defn hive []
  (reify Protocol
    (cmds-required? [_] true)
    (run [_ step]
      (run-hive step))))
