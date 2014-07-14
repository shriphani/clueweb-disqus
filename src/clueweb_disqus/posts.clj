(ns clueweb-disqus.posts
  "Codebase for reading thread listings
   and downloading individual posts"
  (:require [clojure.java.io :as io]
            [clojure.string :as string])
  (:import [java.io File PushbackReader]))

(defn threads-list-files
  [a-directory]
  (map
   #(.getAbsolutePath %)
   (filter
    (fn [f]
      (and
       (re-find #".*-disqus-threads-.*.clj$"
                (.getAbsolutePath f))
       (not
        (re-find #"bkup"
                 (.getAbsolutePath f)))))
    (file-seq
     (File. a-directory)))))

(defn threads-list-seq
  [a-file]
  (let [rdr (-> a-file
                io/reader
                PushbackReader.)]
    (take-while
     identity
     (repeatedly
      (fn []
        (try
          (read rdr)
          (catch Exception e nil)))))))

(defn thread-ids-with-posts
  [a-threads-seq]
  (map
   #(get % "id")
   (filter
    #(< 0 (-> % (get "posts")))
    a-threads-seq)))

(defn dump-thread-ids
  [a-file]
  (let [threads-seq (threads-list-seq a-file)
        thread-ids  (thread-ids-with-posts threads-seq)

        out-file (string/replace a-file #".clj$" ".threads")]
    (with-open [wrtr (io/writer out-file)]
      (doall
       (doseq [thread-id thread-ids]
         (binding [*out* wrtr]
           (println thread-id)))))))

(defn dump-all-thread-ids
  [a-directory]
  (let [files (threads-list-files a-directory)]
    (doseq [a-file files]
      (dump-thread-ids a-file))))
