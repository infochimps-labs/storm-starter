(defproject storm-starter "0.0.1-SNAPSHOT"
  :source-paths ["src/clj"]
  :java-source-paths ["src/jvm"]
  :test-paths ["test/jvm"]
  :resource-paths ["multilang" "conf"]
  :aot :all
  :repositories {
                 "central-1" "http://repo1.maven.org/maven1"
                 "central-2" "http://repo1.maven.org/maven2"
                 "clojars" "http://clojars.org/repo/"
                 "infochimps-releases" "https://s3.amazonaws.com/artifacts.chimpy.us/maven-s3p/releases"
                 "infochimps-snapshots" "https://s3.amazonaws.com/artifacts.chimpy.us/maven-s3p/snapshots"
                 "cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"
                 "github-releases" "http://oss.sonatype.org/content/repositories/github-releases/"
                 "twitter4j" 	"http://twitter4j.org/maven2"
                 }

  :dependencies [
                   [commons-collections/commons-collections "3.2.1"]
                 ]

  :profiles {:dev
              {:dependencies [[storm/storm-core "0.9.0-wip19"]
                              [storm/storm-kafka "0.9.0-wip16a-scala292"]
                              [junit/junit "3.8.1" :scope "test" ]
                              [org.testng/testng "6.8" ]
                              [org.mockito/mockito-all "1.9.0"]
                              [org.easytesting/fest-assert-core "2.0M8"]
                              [org.clojure/clojure "1.4.0"]]}}
  :min-lein-version "2.0.0"
  )
