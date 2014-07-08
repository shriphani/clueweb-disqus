(ns clueweb-disqus.read-stats
  "Read stats + make a graph"
  (:gen-class :main true)
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.io :as io])
  (:use [clojure.pprint :only [pprint]]
        [incanter core stats charts io]))

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

(defn generate-samples
  []
  (let [data-files (filter
                     (fn [f]
                       (re-find #".*-disqus-threads-.*.clj$" (.getAbsolutePath f)))
                     (file-seq (java.io.File. ".")))]
    (str
     "<html>"
     "<body>"
     "<pre>"
     (clojure.string/join
      "\n"
      (map
       (fn [a-data-file]
         (let [rdr (-> a-data-file io/reader java.io.PushbackReader.)
               stream (take-while
                       identity
                       (repeatedly
                        (fn [] (try (read rdr)
                                   (catch Exception e nil)))))]
           (let [w (java.io.StringWriter.)]
             (pprint (last stream) w)
             (.toString w))))
       data-files))
     "</pre>"
     "</body>"
     "</html>")))

(defn -main
  [& args]
  (spit "disqus_stats.csv"
        (str (c/to-long
              (t/now))
             ", "
             (compute-downloaded-records)
             "\n")
        :append true)
  (do (let [data (read-dataset "disqus_stats.csv")
            dates (sel data :cols 0)
            cnt (sel data :cols 1)]
        (save (time-series-plot dates cnt :y-label "Threads List Downloaded")
              "/bos/www/htdocs/spalakod/disqus/disqus_thread_list.png"))
      (spit "/bos/www/htdocs/spalakod/disqus/disqus_samples.html" (generate-samples))))
