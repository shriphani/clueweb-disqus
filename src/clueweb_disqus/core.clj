(ns clueweb-disqus.core
  (:require [cheshire.core :refer :all]
            [clj-http.client :as client]
            [clj-time.coerce :as c]
            [clj-time.core :as t]
            [clj-time.format :as f]
            [clojure.java.io :as io])
  (:use [clojure.pprint :only [pprint]]))

(def thread-request-url "https://disqus.com/api/3.0/threads/list.json")

(def clueweb-start-date-epoch "1328832000")
(def clueweb-end-date-epoch "1336694400")

(def writer-file-name (str "disqus-threads-" (t/now) ".clj"))
(def stats-file-name (str "disqus-threads-" (t/now) ".stats")) ; dump
                                                               ; download stats here

(defn load-credentials
  []
  (-> "credentials.clj"
      slurp
      read-string))

;; 1000 requests allowed per hour

(def session-start-time (atom nil))
(def requests-in-session (atom 0))

(defn get-api-key-for-start-epoch
  [credentials since-epoch]
  (or (-> credentials (get since-epoch) :api-key)
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

(defn create-url
  ([since-epoch]
     (create-url since-epoch nil))
  ([since-epoch cursor]
     (let [credentials (load-credentials)]
       (str thread-request-url
            "?api_key="
            (get-api-key-for-start-epoch credentials (Long/parseLong since-epoch))
            "&since="
            since-epoch
            "&order=asc"
            (if cursor (str "&cursor=" cursor) "")))))

(defn rate-limited-download
  [a-url]
  (cond (nil? @session-start-time)
        (do (swap! session-start-time (fn [x] (t/now))) ; start
                                                               ; the clock
            (swap! requests-in-session inc)
            (-> a-url client/get :body parse-string))

        (and (< (t/in-minutes (t/interval @session-start-time (t/now)))
                60)
             (> 1000 @requests-in-session))
        (do (swap! requests-in-session inc)
            (-> a-url client/get :body parse-string))

        (and (< (t/in-minutes (t/interval @session-start-time (t/now)))
                60)
             (<= 1000 @requests-in-session))
        (let [time-to-sleep (* 1000
                               (-
                                3600
                                (t/in-secs
                                 (t/interval @session-start-time
                                             (t/now)))))]
          (do (Thread/sleep time-to-sleep) ; sleep till the end of the
                                          ; hour
                                          ; and then restart the session
              (swap! session-start-time (fn [x] (t/now)))
              (swap! requests-in-session (fn [x] 1))
              (-> a-url client/get :body parse-string)))))

(defn write-content
  [start-epoch body]
  (let [content-to-write (get body "response")
        filename-to-write (str start-epoch "-" writer-file-name)
        stats-f-to-write (str start-epoch "-" stats-file-name)]
    (do (with-open [wrtr (io/writer filename-to-write :append true)]
          (doall (doseq [c content-to-write]
                   (pprint c wrtr))))
        (if (.exists (java.io.File. stats-f-to-write))
          (let [cnt
                (+ (read-string (slurp stats-f-to-write))
                   (count content-to-write))]
            (spit stats-f-to-write cnt))
          (spit stats-f-to-write (count content-to-write))))))

(defn stop-iteration?
  [start-epoch page-response]
  (let [credentials (load-credentials)
        formatter   (f/formatters :date-hour-minute-second)]
    (<= (-> credentials (get start-epoch) :stop Long/parseLong)
        (try (-> (f/parse formatter
                          (-> page-response last (get "createdAt")))
                 c/to-long
                 (/ 1000))
             (catch Exception e (do (pprint (last page-response))
                                    true)))))) ; stop anyway

(defn discover-iteration
  [start-epoch first-page-content]
  (let [next-pg-check (-> first-page-content
                          (get "cursor")
                          (get "hasNext"))]
    (if next-pg-check
      (let [cursor-value (-> first-page-content
                             (get "cursor")
                             (get "next"))

            next-pg-url (create-url start-epoch
                                    cursor-value)

            next-pg-content (rate-limited-download next-pg-url)]
        (write-content start-epoch next-pg-content)
        (when-not (stop-iteration? start-epoch (get next-pg-content "response"))
          (recur start-epoch
                 next-pg-content))))))

(defn threads-since
  [start-epoch]
  (let [request-url (create-url start-epoch)
        first-page  (rate-limited-download request-url)]
    (write-content start-epoch first-page)
    (discover-iteration start-epoch first-page)))

(defn get-last-written-file
  "There should be only 1 - newer files
   have different start-epoch"
  [start-epoch]
  (first
   (filter
    (fn [x]
      (and (re-find (re-pattern start-epoch)
                    (.getAbsolutePath x))
           (re-find #".clj$" (.getAbsolutePath x))))
    (file-seq
     (java.io.File. ".")))))

(defn threads-since-recover
  "Reads the existing records and restarts the crawl from the next timestamp
   onwards"
  [start-epoch]
  (let [last-file (get-last-written-file start-epoch)
        pb-rdr (-> last-file io/reader java.io.PushbackReader.)
        records-stream (take-while
                        identity
                        (repeatedly (fn [] (try (read pb-rdr)
                                               (catch Exception e nil)))))

        last-record (last records-stream)

        formatter (f/formatters :date-hour-minute-second)]
    (threads-since
     (str
      (quot
       (c/to-long
        (f/parse formatter (get last-record "createdAt")))
       1000)))))
