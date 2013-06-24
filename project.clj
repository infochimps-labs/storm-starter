(defproject storm-starter "0.0.1-SNAPSHOT"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :resource-paths ["multilang" "conf"]
  :aot :all
  :repositories {
;;                 "twitter4j" "http://twitter4j.org/maven2"
                 }

  :dependencies [
;;                 [org.twitter4j/twitter4j-core "2.2.6-SNAPSHOT"]
;;                 [org.twitter4j/twitter4j-stream "2.2.6-SNAPSHOT"]
                   [commons-collections/commons-collections "3.2.1"]
                 ]

  :profiles {:dev
              {:dependencies [[storm/storm-core "0.9.0-wip19-idea"]
                              [org.clojure/clojure "1.4.0"]]}}
  :min-lein-version "2.0.0"
  )
