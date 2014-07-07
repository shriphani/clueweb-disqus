(ns clueweb-disqus.read-stats
  "Read stats + make a graph"
  (:gen-class :main true)
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c])
  (:use [incanter core stats charts io]))

(defn compute-downloaded-records
  []
  (let [stats-files (filter
                     (fn [f]
                       (re-find #".stats$" (.getAbsolutePath f)))
                     (file-seq (java.io.File. ".")))]
    (reduce
     (fn [acc s]
       (+ acc (read-string (slurp s))))
     0
     stats-files)))

(defn -main
  [& args]
  (spit "disqus_stats.csv"
        (str (c/to-long
              (t/now))
             ", "
             (compute-downloaded-records)
             "\n")
        :append true)
  (let [data (read-dataset "disqus_stats.csv")
        dates (sel data :cols 0)
        cnt (sel data :cols 1)]
    (save (time-series-plot dates cnt :y-label "Threads List Downloaded")
          "/bos/www/htdocs/spalakod/disqus/disqus_thread_list.png")))
