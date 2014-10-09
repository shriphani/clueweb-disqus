(ns clueweb-disqus.write
  "Produce some sample disqus warcs"
  (:require [cheshire.core :refer :all]
            [clojure.java.io :as io]
            [clj-time.core :as t])
  (:import [java.io BufferedOutputStream DataOutputStream FileOutputStream PushbackReader StringWriter]
           [java.util.zip GZIPOutputStream]
           [org.clueweb.clueweb12 ClueWeb12WarcRecord])
  (:use [clojure.pprint :only [pprint]]))

(defn read-records
  [record-file]
  (let [rdr (-> record-file io/reader PushbackReader.)
        records
        (take-while
         identity
         (repeatedly
          (fn []
            (try (read rdr)
                 (catch Exception e nil)))))]
    (take 1000 records)))

(defn write-records
  [records out-file]
  (let [output-file (-> out-file
                        io/as-file
                        (FileOutputStream.)
                        (GZIPOutputStream.)
                        (DataOutputStream.))]
    (doseq [record records]
      (let [warc-rec (new ClueWeb12WarcRecord)]
        (doto warc-rec
          (.setWarcRecordType "response")
          (.setWarcContentType "text/json")
          (.setWarcDate (get record "createdAt"))
          (.setContent  (-> record
                            (generate-string {:pretty true})
                            (.getBytes)))
          (.addHeaderMetadata "parent" (str
                                        (or (get record "parent")
                                            (str "thread-"
                                                 (get record "thread"))))))
        (.write warc-rec output-file)))
    (.close output-file)))
