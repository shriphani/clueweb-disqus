(ns clueweb-disqus.main
  "Main namespace from where we control the world"
  (:gen-class :main true)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clueweb-disqus.core :as core]
            [clueweb-disqus.generate-blog-links :as blog]
            [clueweb-disqus.posts :as posts]
            [clueweb-disqus.read-stats :as read-stats]
            [clueweb-disqus.recover :as recover]))

(def options [["-b" "--reboot" "Reboot the crawl by dumping restart points"]
              ["-r" "--recover TS" "Timestamp"]
              ["-s" "--stats" "Read stats"]
              ["-t" "--threads" "Dump a list of relevant thread-ids"]
              ["-d" "--download-threads F" "Download a list of threads"]
              ["-l" "--dump-links" "Dump a list of links"]])

(defn -main
  [& args]
  (let [{options :options} (parse-opts args options)]
    (cond (:recover options)
          (core/threads-since (:recover options))
          
          (:stats options)
          (read-stats/-main)
          
          (:reboot options)
          (recover/dump-restart-points)

          (:threads options)
          (posts/break-up-threads-list)

          (:download-threads options)
          (posts/download-posts-for-threads (:download-threads options))

          (:dump-links options)
          (blog/generate-uris))))
