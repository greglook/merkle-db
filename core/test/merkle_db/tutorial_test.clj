(ns merkle-db.tutorial-test
  (:require
    [clojure.java.io :as io]
    [clojure.string :as str]
    [clojure.test :as test :refer :all]
    [puget.printer :as puget]))


(defn find-code-blocks
  "Pattern match the text to find fenced code blocks. Returns a sequence of
  match vectors containing `[match directive? code]`. The directive may be a
  word appearing inside an HTML comment immediately preceding the block."
  [text]
  (re-seq #"(?:<!-- (\S+) -->\n)?```clojure\n((?:[^`]*\n)*[^`]+)```" text))


(defn split-statements
  "Separate the lines in a code block into individual statements, separated by
  one or more blank lines."
  [code-block]
  (str/split code-block #"\n\n+"))


(defn parse-sections
  "Parse a statement into a map with a `:body` key containing the statement
  and result forms. The map may also contain a `:pre` or `:post` entry with
  leading and trailing comment lines."
  [statement]
  (let [[f s t :as sections]
        (->> (str/split statement #"\n")
             (partition-by #(str/starts-with? % ";")))]
    ; TODO: check block comment case?
    (case (count sections)
      1 {:body f}
      2 (if (str/starts-with? (ffirst sections) ";")
          {:pre f, :body s}
          {:body f, :post s})
      3 {:pre f, :body s, :post t}
      (throw (IllegalStateException.
               (str "Unexpected number of sections in statement: "
                    (count sections) "\n" statement))))))


(defn split-body
  "Separate a the `:body` string in the given statement map into an input
  `:form` and `:expected` value (if present). If the body doesn't begin with
  the prompt string `=>`, it is returned unchanged."
  [statement]
  (let [body (:body statement)]
    (if (str/starts-with? (first body) "=> ")
      ; Split out input form and result.
      (let [indented? #(str/starts-with? % " ")
            form (cons (subs (first body) 3)
                       (take-while indented? (rest body)))
            expected (drop-while indented? (rest body))]
        (-> statement
            (dissoc :body)
            (assoc :form (str/join "\n" form))
            (cond->
              (seq expected) (assoc :expected (str/join "\n" expected)))))
      ; Not a prompt, return unchanged.
      (assoc statement :body (str/join "\n" body)))))


(defn read-form
  "Read the `:form` in the given statement, if present. Otherwise returns the
  statement unchanged."
  [statement]
  (if (:form statement)
    (update statement :form #(binding [*default-data-reader-fn* tagged-literal]
                               (read-string %)))
    statement))


(defn try-eval
  "Attempt to evaluate the given form and use the expected output string and
  any post comments to judge the result. Reports `clojure.test` assertion
  results and returns a vector containing the result and error values, if any."
  [history form expected post]
  (let [err-class (some->> (first post)
                           (re-seq #"^; (\S*Exception) ")
                           (first)
                           (second))]
    (try
      (let [result (binding [*3 (nth history 2 nil)
                             *2 (nth history 1 nil)
                             *1 (nth history 0 nil)]
                     (eval form))]
        (when err-class
          (test/do-report
            {:type :fail
             :message "Expected exception while evaluating form"
             :expected (symbol err-class)
             :actual result}))
        [result nil])
      (catch Exception ex
        (if (= err-class (.getSimpleName (class ex)))
          (do
            (test/do-report
              {:type :pass
               :message (str "Throws " err-class " exception")
               :expected (symbol err-class)
               :actual ex})
            [nil ex])
          (do
            (test/do-report
              {:type :fail
               :message "Unexpected exception while evaluating form"
               :expected (if err-class
                           (symbol err-class)
                           form)
               :actual ex})
            (throw ex)))))))


(deftest evaluate-tutorial
  (io/delete-file "var/playground/refs.tsv" true)
  (loop [history []
         statements (into []
                          (comp
                            (remove #(= "skip-test" (second %)))
                            (map #(nth % 2))
                            (mapcat split-statements)
                            (map parse-sections)
                            (map split-body)
                            (map read-form))
                          (find-code-blocks (slurp "doc/tutorial.md")))]
    (when-let [{:keys [pre body form expected post]} (first statements)]
      (if form
        (let [[result err] (try-eval
                             history
                             form
                             expected
                             post)]
          ;(puget/cprint [form result err])
          ; TODO: assertions on result vs expected
          ; TODO: assert that % of lines different is under some threshold?
          (recur (if-not err
                   (take 3 (cons result history))
                   history)
                 (next statements)))
        (do (println "Ignoring non-statement body:\n"
                     (str/join "\n" body))
            (recur history (next statements)))))))
