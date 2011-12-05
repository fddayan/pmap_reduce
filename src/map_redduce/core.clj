(ns map-redduce.core
	(:use [clojure.tools.logging :only (info error)]))


;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;	Util
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn tokenize-str [text] 
	(re-seq #"\w+" text))
	
(defn count_word [file]
	(let [c (count (tokenize-str (.toLowerCase (slurp file))))]
		{file c}))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;	Internal
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn start-mr-workers [n]
	(map (fn [_] (agent {})) (range n)))
	
(defn count-words-adapter [agent_value file]
	(count_word file))

(defn get-result [workers]
	(map (fn [a] @a) workers))
	
(defn run-plan [dist_fn plan]
	(map 
		(fn [p]
			(let [ arg (p 0) worker_agent (p 1)]
				(send-off worker_agent dist_fn arg)))
		(seq plan)))

(defn dist-map2 [fun arg_list]
	(def workers [(agent {})])
	(def plan  (zipmap arg_list (take (count arg_list ) (cycle workers))))
	(def result (run-plan (fn [data arg] (merge data (fun arg))) plan))
	(apply await result)
	result)

(defn append-to-key [ref_data k v] 
	(def new_data 
		(if (contains? ref_data k) 
			(do (alter (ref_data k) conj v) ref_data)
			(do (assoc ref_data k (ref [v])))))
	new_data)

(defn send-to-reducer [reducers kv] 
	(def k (first kv)) ; (key kv)
	(def v (second kv)) ; (val kv)
	(def i (- 1 (mod (hash k) (count reducers))))
	
	(dosync 
		(alter (reducers i) append-to-key k v))
	reducers)
	
(defn alter-reducer [r_workers kv] 
	(dosync 
		(alter 
			(first r_workers) 
			(fn [ref_data k_arg v_arg] 
				(if (not (contains? ref_data k_arg)) 
					(do (assoc ref_data k_arg (ref [v_arg])))
					(do (alter (ref_data k_arg) conj v_arg) ref_data)))
			(first kv) 
			(second kv))))
	
(defn run-mapper 
	"Execute the mappers and distributes the outout to the reducers"
	[r_workers mapper arg_list]
		(map 
			(fn [arg] 
				(let 
					[agent_mapper (agent {})]
					(send-off agent_mapper 
						(fn [_ x] 
							(let 
								[d (mapper x)] 
								(doseq [i d x i] 
									(alter-reducer r_workers x))
								{})) 
						arg)
					agent_mapper))
			arg_list))
	
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;	API
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;	
	
(defn dist-map [fun arg_list]
	"A distributed version of map. Each item it's executed in a different agent"
	(def result
		(map (fn [arg] 
			(let 
				[worker (agent {})]
				(send-off worker (fn [_ x] (fun x)) arg)
				worker))
			arg_list))

	(apply await result)
	(get-result result))	

(defn distribute-to-reducers [reducers data]
	(doseq [i data j i x j]
			(send-to-reducer reducers x))
	reducers)
	
(defn dist-map-reduce 
	"Distributed map reduce. This version collect data in the mapper agents and then distributes to reducers"
	[mapper reducer arg_list]
	(def reducer_workers [(ref {}) (ref {})])
	
	(def mapper_out
		(map 
			(fn [arg] 
				(let 
					[worker (agent {})]
					(send-off worker 
						(fn [_ x] (mapper x)) arg)
					worker))
			arg_list))	
	
	(apply await mapper_out)
	
	(distribute-to-reducers reducer_workers (vec (get-result mapper_out)))
	
	(def reducers_out 
		(for [rw reducer_workers entry @rw ] 
			(do 
				(reducer (first entry) @(second entry)))))
	
	reducers_out)
	
(defn dist-map-reduce2 [fn_mapper fn_reducer arg_list]
	"Distributed map reduce. This version distributes to the reducers from the mappers agents"
	(def ref_reducers [(ref {}) (ref {})])
	
	(apply await 
		(run-mapper ref_reducers fn_mapper arg_list))		
	
	(for [rw ref_reducers entry @rw ] 
		(fn_reducer (first entry) @(second entry))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;	Apps
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn word-count [text]
	(let [c (tokenize-str (.toLowerCase text))]
		(apply merge-with + (map #(hash-map % 1) c))))
		
(defn word-count-file [file]
	(hash-map file (word-count (slurp file))))		
		
; Returns a list of K,V	
(defn mapper-split-words-text [text] 
	(let [c (tokenize-str (.toLowerCase text))]
		(map #(hash-map % 1) c)))
		
(defn mapper-split-words-file [file]
	(mapper-split-words-text (slurp file)))

; Returns a list K,V 
(defn reducer-count-words [k values]
	(hash-map k (reduce + values)))
	
(defn dist_count_words [files]
	; for each file we create an agent 
	(def workers (start-mr-workers (count files)))

	;to what agent goes each file
	(def plan  (zipmap files (take (count files ) (cycle workers))))

	;we submit a word_count for each file to a single agent	
	(def result (run-plan count-words-adapter plan))

	(apply await result)

	(get-result result))

;(dist-map-reduce mapper-split-words-file reducer-count-words ["/Users/fddayan/.bash_profile","/Users/fddayan/.profile"])
;(dist-map word-count-file ["/Users/fddayan/.bash_profile","/Users/fddayan/.profile"])
;(send-off-to-agents agents fun & args)


; (def apply_round_robin [fun arg1_list arg2_list]
; 	(doseq 
; 		[ i (partition 2 
; 				(interleave 
; 					(cycle arg1_list) 
; 					arg2_list)))]
; 		(fun (first i) (second i))

(defn traverse [fun agents_list args_list]
	(if (not(or(empty? agents_list) (empty? args_list)))
		(do
			(fun (first agents_list) (first args_list))
			(recur (next agents_list) fun (next args_list)))))
			
(defn distribute-to-agents [fun agents_list args_list]
	(traverse 
		(fn [a1 a2] send-off a1 fun a2) 
		(cycle agents_list) args_list))
			
;(distribute-to-agents (repeatedly 3 #(agent [])) #(conj %1 %2) ["/Users/fddayan/.bash_profile","/Users/fddayan/.profile","/Users/fddayan/.bash_profile","/Users/fddayan/.profile"])
