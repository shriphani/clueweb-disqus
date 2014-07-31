(ns clueweb-disqus.read-stats
  "Read stats + make a graph"
  (:gen-class :main true)
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.io :as io]
            [clueweb-disqus.core :as core])
  (:use [clojure.pprint :only [pprint]]
        [incanter core stats charts io]))

(defn compute-downloaded-records
  []
  (let [stats-files (filter
                     (fn [f]
                       (and
                        (not
                         (re-find #"bkup" (.getAbsolutePath f)))
                        (re-find #".stats$" (.getAbsolutePath f))))
                     (file-seq (java.io.File. core/disqus-jobs-dir)))]
    (reduce
     (fn [acc s]
       (+ acc (try (read-string (slurp s))
                   (catch Exception e 0))))
     0
     stats-files)))

(defn compute-downloaded-posts
  []
  (let [downloaded-files (filter
                          (fn [f]
                            (and
                             (not
                              (re-find #"bkup" (.getAbsolutePath f)))
                             (not
                              (re-find #"all.downloaded" (.getAbsolutePath f)))
                             (re-find #".downloaded$" (.getAbsolutePath f))))
                          (file-seq (java.io.File. core/disqus-jobs-dir)))]
    (reduce
     (fn [acc f]
       (+
        acc
        (with-open [rdr (io/reader f)]
          (count
           (line-seq rdr)))))
     0
     downloaded-files)))

(defn generate-samples
  []
  (let [data-files (filter
                    (fn [f]
                      (and
                       (not
                        (re-find #"bkup" (.getAbsolutePath f)))
                       (re-find #".*-disqus-threads-.*.clj$" (.getAbsolutePath f))))
                    (file-seq (java.io.File. core/disqus-jobs-dir)))]
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
  (spit "disqus_post_stats.csv"
        (str (c/to-long
              (t/now))
             ", "
             (compute-downloaded-posts)
             "\n")
        :append true)
  (let [data (read-dataset "disqus_stats.csv")
        dates (sel data :cols 0)
        cnt (sel data :cols 1)

        posts-data (read-dataset "disqus_post_stats.csv")
        posts-dates (sel posts-data :cols 0)
        posts-cnt (sel posts-data :cols 1)]
    (save (time-series-plot dates cnt :y-label "Threads List Downloaded")
          "/bos/www/htdocs/spalakod/disqus/disqus_thread_list.png")
    (save (time-series-plot posts-dates posts-cnt :y-label "Threads List Downloaded")
          "/bos/www/htdocs/spalakod/disqus/disqus_post_list.png")))
