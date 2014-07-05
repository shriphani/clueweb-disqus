(ns clueweb-disqus.core
  (:require [cheshire.core :refer :all]
            [clj-http.client :as client]))

(def thread-request-url "https://disqus.com/api/3.0/threads/list.json")

(def clueweb-start-date-epoch "1328832000")

(defn load-credentials
  []
  (-> "credentials.clj"
      slurp
      read-string))

(defn create-url
  [since-epoch]
  (let [credentials (load-credentials)]
   (str thread-request-url
        "?api_key="
        (-> credentials (get since-epoch) :api-key)
        "&since="
        since-epoch)))

(defn threads-since
  [start-epoch]
  (let [request-url (create-url start-epoch)
        first-page  (-> request-url client/get :body parse-string)]
    first-page))
