(ns clueweb-disqus.core
  (:require [cheshire.core :refer :all]
            [clj-http.client :as client]
            [clj-time.core :as t]
            [clojure.java.io :as io])
  (:use [clojure.pprint :only [pprint]]))

(def thread-request-url "https://disqus.com/api/3.0/threads/list.json")

(def clueweb-start-date-epoch "1328832000")
(def clueweb-end-date-epoch "1336694400")

(def writer-file-name (str "disqus-threads-" (t/now) ".clj"))

(defn load-credentials
  []
  (-> "credentials.clj"
      slurp
      read-string))

;; 1000 requests allowed per hour

(def session-start-time (atom nil))
(def requests-in-session (atom 0))

(defn create-url
  ([since-epoch]
     (create-url since-epoch nil))
  ([since-epoch cursor]
     (let [credentials (load-credentials)]
       (str thread-request-url
            "?api_key="
            (-> credentials (get since-epoch) :api-key)
            "&since="
            since-epoch
            (if cursor (str "&cursor=" cursor) "")))))

(defn rate-limited-download
  [a-url]
  (cond (nil? @session-start-time)
        (do (swap! session-start-time (fn [x] (t/now))) ; start
                                                               ; the clock
            (swap! requests-in-session inc)
            (-> a-url client/get :body parse-string))

        (and (< (t/in-minutes (t/duration @session-start-time t/now))
                60)
             (> 1000 @requests-in-session))
        (do (swap! requests-in-session inc)
            (-> a-url client/get :body parse-string))

        (and (< (t/in-minutes (t/duration @session-start-time t/now))
                60)
             (<= 1000 @requests-in-session))
        (let [time-to-sleep (* 1000
                               (-
                                3600
                                (t/in-secs
                                 (t/duration @session-start-time
                                             (t/now)))))]
          (do (Thead/sleep time-to-sleep) ; sleep till the end of the
                                          ; hour
                                          ; and then restart the session
              (swap! session-start-time (fn [x] (t/now)))
              (swap! requests-in-session (fn [x] 1))
              (-> a-url client/get :body parse-string)))))

(defn write-content
  [start-epoch body]
  (let [content-to-write (get body "response")
        filename-to-write (str start-epoch "-" writer-file-name)]
    (with-open [wrtr (io/writer filename-to-write :append true)]
      (pprint content-to-write wrtr))))

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
        (recur start-epoch
               next-pg-content)))))

(defn threads-since
  [start-epoch]
  (let [request-url (create-url start-epoch)
        first-page  (rate-limited-download request-url)]
    (write-content start-epoch first-page)
    (discover-iteration start-epoch first-page-content)))
