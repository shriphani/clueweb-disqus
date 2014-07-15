(ns clueweb-disqus.recover
  "Module to restart crawls"
  (:require [clj-time.coerce :as c]
            [clj-time.format :as f]
            [clj-time.core :as t]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clueweb-disqus.core :as core]
            [clueweb-disqus.posts :as posts])
  (:use [clojure.java.shell :only [sh]])
  (:import [java.lang ProcessBuilder]))

(defn list-disqus-files
  []
  (map
   #(.getAbsolutePath %)
   (filter
    (fn [f]
      (and
       (not
        (re-find #"bkup"
                 (.getAbsolutePath f)))
       (re-find #".*-disqus-threads.*.clj"
                (.getAbsolutePath f))))
    (file-seq
     (io/as-file core/disqus-jobs-dir)))))

(defn potential-restart-point
  [a-disqus-file]
  (let [formatter   (f/formatters :date-hour-minute-second)]
    (quot (c/to-long
           (f/parse formatter
                    (last
                     (map
                      #(get % "createdAt")
                      (posts/threads-list-seq a-disqus-file)))))
          1000)))

(defn potential-restart-points
  []
  (pmap potential-restart-point
        (list-disqus-files)))

(defn dump-restart-points
  []
  (let [credentials  (core/load-credentials)
        starts       (keys credentials)
        starts-stops (map
                      (fn [x]
                        [(Long/parseLong x)
                         (Long/parseLong
                          (:stop
                           (credentials x)))])
                      starts)

        restart-points (potential-restart-points)

        chosen-restart-epochs (pmap
                               (fn [[start stop]]
                                 (apply
                                  max
                                  (filter
                                   #(and
                                     (<= start %)
                                     (> stop %))
                                   restart-points)))
                               starts-stops)]
    
    (with-open [wrtr (io/writer (str core/disqus-jobs-dir "recover.clj"))]
     (doseq [epoch chosen-restart-epochs]
       (binding [*out* wrtr]
        (println epoch))))))
