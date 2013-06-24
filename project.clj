(defproject storm-starter "0.0.1-SNAPSHOT"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/jvm"]
  :resource-paths ["multilang" "conf"]
  :aot :all
  :repositories {
;;                 "twitter4j" "http://twitter4j.org/maven2"
                 "central-1" "http://repo1.maven.org/maven1"
                 "central-2" "http://repo1.maven.org/maven2"
                 "clojars" "http://clojars.org/repo/"
                 "infochimps-releases" "https://s3.amazonaws.com/artifacts.chimpy.us/maven-s3p/releases"
                 "infochimps-snapshots" "https://s3.amazonaws.com/artifacts.chimpy.us/maven-s3p/snapshots"
                 "cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"
                 }

  :dependencies [
;;                 [org.twitter4j/twitter4j-core "2.2.6-SNAPSHOT"]
;;                 [org.twitter4j/twitter4j-stream "2.2.6-SNAPSHOT"]
                   [commons-collections/commons-collections "3.2.1"]
                   [storm/storm-kafka "0.9.0-wip16a-scala292"]
                 ]

  :profiles {:dev
              {:dependencies [[storm/storm-core "0.9.0-wip19-idea"]
                              ;[org.easytesting/fest "1.0.16"]
                              [org.clojure/clojure "1.4.0"]]}}
  :min-lein-version "2.0.0"
  )
