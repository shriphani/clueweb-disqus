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

(def disqus-jobs-dir "/bos/tmp19/spalakod/clueweb12pp/disqus/")

(def recovery-file (str disqus-jobs-dir "recover.clj"))

(defn load-credentials
  []
  (-> "credentials.clj"
      slurp
      read-string))

;; 1000 requests allowed per hour

(def session-start-time (atom nil))
(def current-hour (atom (t/hour (t/now))))
(def requests-in-session (atom 0))
(def time-of-last-request (atom nil))

(defn till-end-of-hour-minutes
  "Measures the time remaining in milliseconds
   for the current hour to end"
  []
  (let [current-time (t/now)]
    (- 59 (t/minute current-time))))

(defn till-end-of-hour-seconds
  []
  (* 60 (till-end-of-hour-minutes)))

(defn till-end-of-hour-millis
  []
  (* 1000 (till-end-of-hour-seconds)))

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

(defn get-stop-for-start-epoch
  [credentials since-epoch]
  (or (try (-> credentials (get (str since-epoch)) :stop Long/parseLong)
           (catch Exception e nil))
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
        (-> credentials (get correct-key) :stop Long/parseLong))))

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
            "&limit=100"
            (if cursor (str "&cursor=" cursor) "")))))

(defn make-request
  [a-url]
  (-> a-url client/get :body parse-string))

(defn rate-limited-download
  [a-url]
  (cond (nil? @session-start-time)
        (do (swap! session-start-time (fn [x] (t/now))) ; start
                                        ; the clock
            (swap! requests-in-session inc)
            (swap! time-of-last-request (fn [t] (t/now)))
            (make-request a-url))

        ;; if the hour changes, new session
        (and (= (t/hour (t/now))
                (t/hour @session-start-time))
             (> 1000 @requests-in-session))
        (do (swap! requests-in-session inc)
            (make-request a-url))

        (and (= (t/hour (t/now))
                (t/hour @session-start-time))
             (<= 1000 @requests-in-session))
        (let [time-to-sleep (till-end-of-hour-millis)]
          (do (Thread/sleep time-to-sleep) ; sleep till the end of the
                                        ; hour
                                        ; and then restart the session
              (swap! session-start-time (fn [x] (t/now)))
              (swap! requests-in-session (fn [x] 1))
              (make-request a-url)))

        ;; the clock ticked over
        (not= (t/hour (t/now))
              (t/hour @session-start-time))
        (do (swap! session-start-time (fn [x] (t/now)))
            (swap! requests-in-session (fn [x] 1))
            (make-request a-url))

        :else
        (do (swap! requests-in-session inc)
            (make-request a-url))))

(defn write-content
  [start-epoch body]
  (let [content-to-write (get body "response")
        filename-to-write (str disqus-jobs-dir start-epoch "-" writer-file-name)
        stats-f-to-write (str disqus-jobs-dir start-epoch "-" stats-file-name)
        threads-file (clojure.string/replace filename-to-write #".clj$" ".threads")]
    (do (with-open [wrtr (io/writer filename-to-write :append true)
                    threads-wrtr (io/writer threads-file :append true)]
          (doall
           (doseq [c content-to-write]
             (pprint c wrtr)
             (println (get c "id") threads-wrtr))))
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
    (try
      (<= (get-stop-for-start-epoch credentials (Long/parseLong start-epoch))
         (-> (f/parse formatter
                      (-> page-response last (get "createdAt")))
             c/to-long
             (/ 1000)))
      (catch Exception e (do (println (.getMessage e))
                             (pprint (last page-response))
                             (flush)
                             true)))))              ; stop anyway

(defn process-disqus-date
  [a-date]
  (let [formatter (f/formatters :date-hour-minute-second)]
    (-> (f/parse formatter a-date)
        c/to-long
        (quot 1000))))

(declare threads-since)

(defn discover-iteration
  [start-epoch first-page-content]
  (let [credentials   (load-credentials)
        next-pg-check (-> first-page-content
                          (get "cursor")
                          (get "hasNext"))]
    (cond  next-pg-check ; pagination exists
           (let [cursor-value (-> first-page-content
                                  (get "cursor")
                                  (get "next"))
                 
                 next-pg-url (create-url start-epoch
                                         cursor-value)
                 
                 next-pg-content (rate-limited-download next-pg-url)]
             (write-content start-epoch next-pg-content)
             (cond (nil? (last (get next-pg-content "response"))) ; if nil,
                                        ; even requerying was useless.
                                        ; so we reboot the crawl
                   (do (println :nil-returned :rebooting-crawl)
                       (spit recovery-file
                             (-> first-page-content
                                 (get "response")
                                 last
                                 (get "createdAt")
                                 process-disqus-date
                                 (str "\n"))
                             :append
                             true)
                       (threads-since (-> first-page-content
                                          (get "response")
                                          last
                                          (get "createdAt")
                                          process-disqus-date
                                          str)))

                   (not
                    (stop-iteration? start-epoch (get next-pg-content "response")))
                   (recur start-epoch
                          next-pg-content)))

           ;; disqus says pagination has ended but it clearly
           ;; hasn't. Reboot the crawl here.
           (and (not next-pg-check)
                (<= (-> first-page-content
                        (get "response")
                        last
                        (get "createdAt")
                        process-disqus-date)
                    (get-stop-for-start-epoch credentials (Long/parseLong start-epoch))))

           ;; simply reboot the crawl here
           (do (println :pagination-finished :crawl-not-finished)
               (threads-since (-> first-page-content
                                  (get "response")
                                  last
                                  (get "createdAt")
                                  process-disqus-date
                                  str)))

           (and (not next-pg-check)
                (> (-> first-page-content
                       (get "response")
                       last
                       (get "createdAt")
                       process-disqus-date)
                   (get-stop-for-start-epoch credentials (Long/parseLong start-epoch))))
           (println :pagination-finished :crawl-also-finished))))

(defn threads-since
  [start-epoch]
  (let [request-url (create-url start-epoch)
        first-page  (rate-limited-download request-url)]
    (if (nil? (get first-page "response"))
      (println :motherfucker-returned-nil :threads-since start-epoch)
      (do (write-content start-epoch first-page)
          (discover-iteration start-epoch first-page)))))

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
     (java.io.File. disqus-jobs-dir)))))

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
