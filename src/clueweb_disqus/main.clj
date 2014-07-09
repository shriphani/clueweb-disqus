(ns clueweb-disqus.main
  "Main namespace from where we control the world"
  (:gen-class :main true)
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clueweb-disqus.core :as core]
            [clueweb-disqus.read-stats :as read-stats]))

(def options [["-r" "--recover TS" "Timestamp"]
              ["-s" "--stats" "Read stats"]])

(defn -main
  [& args]
  (let [{options :options} (parse-opts args options)]
    (cond (:recover options)
          (core/threads-since (:recover options))

          (:stats options)
          (read-stats/-main))))
