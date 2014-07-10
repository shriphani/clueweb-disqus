(ns clueweb-disqus.recover
  "Module to restart crawls"
  (:require [clojure.string :as string]
            [clueweb-disqus.core :as core])
  (:use [clojure.java.shell :only [sh]])
  (:import [java.lang ProcessBuilder]))

(defn to-recover
  []
  (string/split-lines
   (slurp core/recovery-file)))

(defn restart-to-recover-jobs
  []
  (do (let [to-recover-list (to-recover)]
        (doseq [disqus-proc to-recover-list]
          (sh "lein" (str "run -r " disqus-proc))))
      (spit core/recovery-file "")))
