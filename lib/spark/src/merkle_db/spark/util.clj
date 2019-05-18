(ns merkle-db.spark.util
  "Internal Spark utilities."
  (:import
    org.apache.spark.SparkContext
    org.apache.spark.api.java.JavaSparkContext))


(defmacro with-op-scope
  "Apply a Spark context operation scope around the statements in the body."
  [spark-ctx scope-name & body]
  `(.withScope org.apache.spark.rdd.RDDOperationScope$/MODULE$
     ~spark-ctx ~scope-name true false
     (proxy [scala.Function0] []
       (apply [] ~@body))))


(defn set-job-group!
  "Assigns a group identifier and description to all jobs started by this
  thread until the group is changed or cleared. Once set, the Spark web UI will
  associate such jobs with this group."
  [^SparkContext spark-ctx group-id group-description]
  (.setJobGroup spark-ctx group-id group-description false))


(defn clear-job-group!
  "Clear the current thread's job group ID and its description."
  [^SparkContext spark-ctx]
  (.clearJobGroup spark-ctx))


(defmacro with-job-group
  "Run the given body of actions within a specific job group."
  [spark-ctx group-id group-description & body]
  `(let [ctx# ~spark-ctx]
     (try
       (set-job-group! ctx# ~group-id ~group-description)
       ~@body
       (finally
         (clear-job-group! ctx#)))))
