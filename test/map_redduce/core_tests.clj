(ns map-redduce.core)
(use 'clojure.test)
;(use 'clojure.tools.trace)
	

(deftest test-tokenize-str 
	(is (= ["a" "cow" "is" "a" "cow"] (tokenize-str "a cow is a cow"))))
	
	
(deftest test-dist-map-reduce
	(defn mapper [arg] (map #(hash-map % 1) (tokenize-str arg)))
	(defn reducer [k values] (hash-map k (reduce + values)))
	
	(is (= [{"donky" 1} {"cow" 1} {"a" 2} {"is" 2}] (dist-map-reduce mapper reducer ["is a cow","is a donky"]))))

 
(deftest test-dist-map-reduce2
	(defn mapper [arg] (map #(hash-map % 1) (tokenize-str arg)))
	(defn reducer [k values] (hash-map k (reduce + values)))
	
	(is (= [{"a" 2} {"is" 2} {"cow" 1} {"donky" 1}] (sort #(compare (hash %1) (hash %2)) (dist-map-reduce2 mapper reducer ["is a cow","is a donky"])))))

(deftest test-run-mapper
	(defn mapper [arg] (map #(hash-map % 1) (tokenize-str arg)))
	(defn reducer [k values] (hash-map k (reduce + values)))
	(def r_workers [(ref {})])
	
	(apply await 
		(run-mapper r_workers mapper ["is a cow","is a donky"]))

	(is (= 4 (count @(first r_workers)))))
;	(is (= [{"donky" 1} {"cow" 1} {"a" 1} {"is" 1}] @(first r_workers))))

(deftest test-mapper-split-words-text
	(is (= [{"a" 1} {"cow" 1} {"is" 1} {"a" 1} {"cow" 1}] (mapper-split-words-text "a cow is a cow"))))
	
		

