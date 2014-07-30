(ns clueweb-disqus.generate-blog-links
  "Blog web corpus"
  (:require [cheshire.core :as json]
            [clojure.java.io :as io]
            [clueweb-disqus.core :as core]
            [clueweb-disqus.posts :as posts])
  (:import [java.io PushbackReader]))

(defn dump-permalinks
  [a-file out-file]
  (let [out-fd (io/writer out-file :append true)
        records-stream (posts/threads-list-seq a-file)]
    (doseq [a-record records-stream]
      (binding [*out* out-fd]
        (-> a-record
            (get "link")
            println)))))

(defn generate-uris
  []
  (let [files (filter
               (fn [f]
                 (and (re-find
                       #".*-disqus-threads-.*.clj$"
                       (.getAbsolutePath f))
                      (not
                       (re-find
                        #"bkup"
                        (.getAbsolutePath f)))))
               (file-seq
                (io/as-file core/disqus-jobs-dir)))

        out-file (str core/disqus-jobs-dir "permalinks.txt")]
    
    (doseq [file files]
      (dump-permalinks file out-file))))
