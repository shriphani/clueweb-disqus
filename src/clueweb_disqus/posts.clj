(ns clueweb-disqus.posts
  "Codebase for reading thread listings
   and downloading individual posts"
  (:require [clj-time.core :as t]
            [clj-time.coerce :as c]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [clueweb-disqus.core :as core])
  (:import [java.io File PushbackReader])
  (:use [clojure.pprint :only [pprint]]))

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

(defn thread-ids-list-files
  [a-directory]
  (map
   #(.getAbsolutePath %)
   (filter
    (fn [f]
      (and
       (re-find #".*-disqus-threads-.*.threads$"
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

(def posts-endpoint "https://disqus.com/api/3.0/posts/list.json")

(defn create-url
  ([thread-id api-key]
     (create-url thread-id api-key nil))
  ([thread-id api-key cursor]
     (str posts-endpoint
          "?api_key="
          api-key
          "&thread="
          thread-id
          "&order=asc"
          "&limit=100"
          (if cursor (str "&cursor=" cursor) ""))))

(defn create-url-2
  ([thread-ids api-key]
     (create-url-2 thread-ids api-key nil))
  ([thread-ids api-key cursor]
     (str posts-endpoint
          "?api_key="
          api-key
          (apply
           str
           (map
            (fn [t]
              (str "&thread=" t))
            thread-ids))
          "&order=asc"
          "&limit=100"
          (if cursor (str "&cursor=" cursor) ""))))

(defn write-content
  [filename-to-write body]
  (let [content-to-write (get body "response")]
    (do (with-open [wrtr (io/writer filename-to-write :append true)]
          (doall
           (doseq [c content-to-write]
             (pprint c wrtr)))))))

(defn discover-iteration
  [thread-id filename-to-write first-page-content api-key]
  (let [next-pg-check (-> first-page-content
                          (get "cursor")
                          (get "hasNext"))]
    (if next-pg-check ; pagination exists
      (let [cursor-value (-> first-page-content
                             (get "cursor")
                             (get "next"))
            
            next-pg-url (create-url-2 thread-id
                                      api-key
                                      cursor-value)
            
            next-pg-content (core/rate-limited-download next-pg-url)]
        (write-content filename-to-write
                       next-pg-content)
        (recur thread-id filename-to-write next-pg-content api-key))
      
      (println :thread-posts-finished))))

(defn download-posts
  "Download all the posts for a single thread"
  [thread-id file-to-write api-key]
  (let [url (create-url-2 thread-id
                          api-key)
        first-page-content (core/rate-limited-download url)]
    (write-content file-to-write first-page-content)
    (discover-iteration thread-id file-to-write first-page-content api-key)))

(defn load-credentials
  []
  (-> "credentials-posts.clj"
      slurp
      read-string))

(defn get-api-key-for-start-epoch
  [credentials since-epoch]
  (or (-> credentials (get (str since-epoch)) :api-key)
      (let [orig-epochs (keys credentials)
            correct-key (str
                         (first
                          (last
                           (sort-by
                            first
                            (filter
                             (fn [[e d]]
                               (pos? d))
                             (map
                              vector
                              (map #(Long/parseLong %) orig-epochs)
                              (map #(- since-epoch (Long/parseLong %))
                                   orig-epochs)))))))]
        (-> credentials (get correct-key) :api-key))))

(defn download-posts-for-threads
  [thread-ids-file]
  (let [file-to-write (string/replace thread-ids-file
                                      #".threads$"
                                      ".posts.lock")

        final-file (string/replace thread-ids-file
                                   #".threads$"
                                   ".posts")

        associated-epoch (first
                          (string/split
                           (last
                            (string/split thread-ids-file #"/"))
                           #"-"))
        
        credentials (load-credentials)

        deja-downloaded-file (string/replace thread-ids-file
                                             #".threads$"
                                             ".downloaded")]
    (if-not (.exists
             (io/as-file file-to-write))
      
      ;; rename existing file
      (do (when (.exists
                 (io/as-file final-file))
            (.rename (io/as-file final-file)
                     (io/as-file file-to-write)))
          
          ;; kick off download
          (let [thread-ids
                (partition
                 300
                 (string/split-lines
                  (slurp thread-ids-file)))]
            (do (doseq [thread-id thread-ids]
                  (download-posts thread-id
                                  file-to-write
                                  (get-api-key-for-start-epoch
                                   credentials
                                   (Long/parseLong associated-epoch))))

                ;; rename back the file
                (.rename (io/as-file file-to-write)
                         (io/as-file final-file))

                ;; now record the download thread-ids
                (with-open [wrtr (io/writer deja-downloaded-file :append true)]
                  (doall
                   (doseq [thread-id thread-ids]
                     (binding [*out* wrtr]
                       (println thread-id))))))))

      (println :crawl-in-progress))))

(defn break-up-threads-list
  []
  (let [threads-listing (str core/disqus-jobs-dir "all.threads")
        downloaded-listing (if (.exists
                                (io/as-file (str core/disqus-jobs-dir "all.downloaded")))
                            (set
                             (string/split-lines
                              (slurp (str core/disqus-jobs-dir "all.downloaded"))))
                            (set []))
        thread-stream (with-open [rdr (io/reader threads-listing)]
                        (doall
                         (filter
                          (fn [x]
                           (not
                            (some #{x} downloaded-listing)))
                          (line-seq rdr))))

        num-threads (count thread-stream)

        size-of-batches (quot num-threads 8)

        credentials (keys
                     (load-credentials))

        keys-threads-stream 
        (map vector credentials (partition size-of-batches thread-stream))]

    (doseq [[k ts] keys-threads-stream]
      (with-open [wrtr (io/writer
                        (str core/disqus-jobs-dir k "-thread-ids" ".threads")
                        :append
                        true)]
        (doall
         (doseq [t ts]
           (binding [*out* wrtr]
             (println t))))))))
