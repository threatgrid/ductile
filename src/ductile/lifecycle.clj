(ns ductile.lifecycle
  "Lifecycle management abstraction for ILM (Elasticsearch) and ISM (OpenSearch).

   This module provides transformation between Elasticsearch's ILM (Index Lifecycle Management)
   and OpenSearch's ISM (Index State Management) policy formats, allowing the same policy
   API to work transparently with both engines."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ductile.conn :refer [make-http-opts safe-es-read]]
            [ductile.features :as feat]
            [ductile.schemas :refer [ESConn Policy]]
            [ductile.uri :as uri]
            [schema.core :as s]))

;; ============================================================================
;; Schemas
;; ============================================================================

(s/defschema ILMPhase
  "ILM phase structure"
  (s/conditional
   #(contains? % :actions)
   {(s/optional-key :min_age) s/Str
    :actions {s/Keyword s/Any}}
   :else
   {s/Keyword s/Any}))

(s/defschema ILMPolicy
  "Elasticsearch ILM policy structure"
  {:phases {s/Keyword ILMPhase}})

(s/defschema ISMTransition
  "OpenSearch ISM transition structure"
  {(s/optional-key :state_name) s/Str
   (s/optional-key :conditions) {s/Keyword s/Any}})

(s/defschema ISMAction
  "OpenSearch ISM action structure"
  (s/conditional
   map? {s/Keyword s/Any}
   :else s/Any))

(s/defschema ISMState
  "OpenSearch ISM state structure"
  {:name s/Str
   :actions [ISMAction]
   (s/optional-key :transitions) [ISMTransition]})

(s/defschema ISMPolicy
  "OpenSearch ISM policy structure"
  {:states [ISMState]
   (s/optional-key :description) s/Str
   (s/optional-key :default_state) s/Str
   (s/optional-key :schema_version) s/Int
   (s/optional-key :ism_template) [{s/Keyword s/Any}]})

;; ============================================================================
;; ILM Action Transformation
;; ============================================================================

(defn- transform-rollover-action
  "Transform ILM rollover action to ISM format"
  [rollover-config]
  {:rollover
   (cond-> {}
     (:max_age rollover-config)
     (assoc :min_index_age (:max_age rollover-config))

     (:max_docs rollover-config)
     (assoc :min_doc_count (:max_docs rollover-config))

     (:max_size rollover-config)
     (assoc :min_size (:max_size rollover-config)))})

(defn- transform-delete-action
  "Transform ILM delete action to ISM format"
  [_delete-config]
  {:delete {}})

(defn- transform-readonly-action
  "Transform ILM readonly action to ISM format"
  [_readonly-config]
  {:read_only {}})

(defn- transform-shrink-action
  "Transform ILM shrink action to ISM format"
  [shrink-config]
  {:shrink
   (cond-> {}
     (:number_of_shards shrink-config)
     (assoc :num_new_shards (:number_of_shards shrink-config)))})

(defn- transform-force-merge-action
  "Transform ILM force_merge action to ISM format"
  [force-merge-config]
  {:force_merge
   (cond-> {}
     (:max_num_segments force-merge-config)
     (assoc :max_num_segments (:max_num_segments force-merge-config)))})

(defn- transform-ilm-action
  "Transform a single ILM action to ISM format"
  [[action-name action-config]]
  (case action-name
    :rollover (transform-rollover-action action-config)
    :delete (transform-delete-action action-config)
    :readonly (transform-readonly-action action-config)
    :shrink (transform-shrink-action action-config)
    :force_merge (transform-force-merge-action action-config)
    ;; Unsupported actions are logged but not transformed
    (do
      (when-not (#{:set_priority :allocate :migrate} action-name)
        (log/warn "Unsupported ILM action:" action-name))
      nil)))

(defn- transform-ilm-actions
  "Transform ILM actions map to ISM actions array"
  [actions]
  (->> actions
       (map transform-ilm-action)
       (filter some?)
       vec))

;; ============================================================================
;; Phase to State Transformation
;; ============================================================================

(def phase-order
  "Standard ILM phase order"
  [:hot :warm :cold :frozen :delete])

(defn- parse-min-age
  "Parse min_age string like '7d' or '30d' into ISM condition format"
  [min-age-str]
  (when min-age-str
    (let [pattern #"(\d+)(ms|s|m|h|d)"
          [_ num unit] (re-matches pattern min-age-str)]
      (when num
        (str num unit)))))

(defn- get-next-phase
  "Get the next phase in the lifecycle"
  [current-phase phases]
  (let [current-idx (.indexOf ^java.util.List phase-order current-phase)
        next-phases (drop (inc current-idx) phase-order)]
    (first (filter #(contains? phases %) next-phases))))

(defn- create-transition
  "Create ISM transition to next state"
  [next-phase min-age]
  (when next-phase
    (let [transition {:state_name (name next-phase)}]
      (if min-age
        (assoc transition :conditions {:min_index_age min-age})
        transition))))

(s/defn transform-ilm-to-ism :- ISMPolicy
  "Transform Elasticsearch ILM policy to OpenSearch ISM policy.

   ILM uses phases (hot, warm, cold, delete) with actions.
   ISM uses states with actions and transitions between states.

   Example:
     (transform-ilm-to-ism
       {:phases {:hot {:actions {:rollover {:max_docs 100000}}}
                 :delete {:min_age \"30d\" :actions {:delete {}}}}})"
  [ilm-policy :- ILMPolicy]
  (let [phases (:phases ilm-policy)
        sorted-phases (filter #(contains? phases %) phase-order)
        states (for [phase sorted-phases]
                 (let [phase-config (get phases phase)
                       actions (transform-ilm-actions (:actions phase-config))
                       next-phase (get-next-phase phase phases)
                       next-phase-config (when next-phase (get phases next-phase))
                       min-age (parse-min-age (:min_age next-phase-config))
                       transition (create-transition next-phase min-age)]
                   (cond-> {:name (name phase)
                            :actions actions}
                     transition (assoc :transitions [transition]))))]
    {:states (vec states)
     :description "Transformed from ILM policy"
     :default_state (name (first sorted-phases))
     :schema_version 1}))

;; ============================================================================
;; ISM to ILM Transformation (Reverse)
;; ============================================================================

(defn- transform-ism-rollover-action
  "Transform ISM rollover action to ILM format"
  [rollover-config]
  {:rollover
   (cond-> {}
     (:min_index_age rollover-config)
     (assoc :max_age (:min_index_age rollover-config))

     (:min_doc_count rollover-config)
     (assoc :max_docs (:min_doc_count rollover-config))

     (:min_size rollover-config)
     (assoc :max_size (:min_size rollover-config)))})

(defn- transform-ism-action
  "Transform a single ISM action to ILM format"
  [action]
  (cond
    (:rollover action) (transform-ism-rollover-action (:rollover action))
    (:delete action) {:delete {}}
    (:read_only action) {:readonly {}}
    (:shrink action) {:shrink {:number_of_shards (get-in action [:shrink :num_new_shards])}}
    (:force_merge action) {:force_merge {:max_num_segments (get-in action [:force_merge :max_num_segments])}}
    :else nil))

(s/defn transform-ism-to-ilm :- ILMPolicy
  "Transform OpenSearch ISM policy to Elasticsearch ILM policy.

   This is a best-effort transformation as ISM is more flexible than ILM.
   Some ISM features may not have direct ILM equivalents."
  [ism-policy :- ISMPolicy]
  (let [states (:states ism-policy)
        phases (reduce
                (fn [acc state]
                  (let [state-name (keyword (:name state))
                        actions (:actions state)
                        ilm-actions (reduce
                                     (fn [acts action]
                                       (if-let [transformed (transform-ism-action action)]
                                         (merge acts transformed)
                                         acts))
                                     {}
                                     actions)
                        transitions (:transitions state)
                        min-age (get-in (first transitions) [:conditions :min_index_age])]
                    (assoc acc state-name
                           (cond-> {:actions ilm-actions}
                             min-age (assoc :min_age min-age)))))
                {}
                states)]
    {:phases phases}))

;; ============================================================================
;; Public API - Policy Transformation
;; ============================================================================

(defn normalize-policy
  "Normalize a policy for the target engine.
   If the policy is already in the target format, return as-is.
   Otherwise, transform it."
  [policy target-engine]
  (case target-engine
    :opensearch (if (contains? policy :states)
                  policy
                  (transform-ilm-to-ism policy))
    :elasticsearch (if (contains? policy :phases)
                     policy
                     (transform-ism-to-ilm policy))
    policy))

;; ============================================================================
;; Public API - Policy HTTP Operations
;; ============================================================================

(s/defn policy-uri :- s/Str
  "Make a policy URI from a host, policy name, and engine type.
   - Elasticsearch uses _ilm/policy
   - OpenSearch uses _plugins/_ism/policies"
  ([uri :- s/Str
    policy-name :- s/Str
    engine :- s/Keyword]
   (case engine
     :elasticsearch (uri/uri uri "_ilm/policy" (uri/uri-encode policy-name))
     :opensearch (uri/uri uri "_plugins/_ism/policies" (uri/uri-encode policy-name))
     ;; Default to ILM for backward compatibility
     (uri/uri uri "_ilm/policy" (uri/uri-encode policy-name))))
  ([uri :- s/Str
    policy-name :- s/Str]
   ;; Backward compatibility: default to Elasticsearch ILM
   (policy-uri uri policy-name :elasticsearch)))

(s/defn get-policy-raw
  "Get a lifecycle management policy with full response metadata.
   Returns the raw response including _seq_no and _primary_term for OpenSearch.
   Use this when you need version info for updates."
  [{:keys [uri version engine request-fn] :as conn} :- ESConn
   policy-name :- s/Str]
  (when-not (feat/lifecycle-management-type conn)
    (throw (ex-info "Lifecycle management not supported"
                    {:engine engine :version version})))
  (-> (make-http-opts conn)
      (assoc :method :get
             :url (policy-uri uri policy-name engine))
      request-fn
      safe-es-read))

(s/defn update-policy!
  "Update an existing lifecycle management policy.
   - For Elasticsearch: Updates an ILM policy (simple PUT, no version control needed)
   - For OpenSearch: Updates an ISM policy using seq_no and primary_term for optimistic locking

   The policy parameter should be in ILM format. It will be automatically
   transformed to ISM format if connecting to OpenSearch.

   For OpenSearch, this function first retrieves the current policy to get
   _seq_no and _primary_term, then performs a conditional update."
  [{:keys [uri version engine request-fn] :as conn} :- ESConn
   policy-name :- s/Str
   policy :- Policy]
  (when-not (feat/lifecycle-management-type conn)
    (throw (ex-info "Lifecycle management not supported"
                    {:engine engine :version version})))

  (case engine
    :elasticsearch
    ;; Elasticsearch ILM allows simple PUT to update
    (let [response (-> (make-http-opts conn {} [] {:policy policy} nil)
                       (assoc :method :put
                              :url (policy-uri uri policy-name engine))
                       request-fn
                       safe-es-read)]
      response)

    :opensearch
    ;; OpenSearch ISM requires seq_no and primary_term for updates
    (let [existing (get-policy-raw conn policy-name)]
      (when-not existing
        (throw (ex-info "Policy not found for update"
                        {:policy-name policy-name
                         :engine engine})))
      (let [seq-no (:_seq_no existing)
            primary-term (:_primary_term existing)
            normalized-policy (normalize-policy policy engine)
            update-url (str (policy-uri uri policy-name engine)
                            "?if_seq_no=" seq-no
                            "&if_primary_term=" primary-term)
            response (-> (make-http-opts conn {} [] {:policy normalized-policy} nil)
                         (assoc :method :put
                                :url update-url)
                         request-fn
                         safe-es-read)]
        (if (:_id response)
          {:acknowledged true}
          response)))

    ;; Default fallback
    (throw (ex-info "Unknown engine type"
                    {:engine engine}))))

(defn- policy-conflict?
  "Check if an exception is a 409 conflict error (policy already exists)"
  [ex]
  (when-let [es-res (-> ex ex-data :es-http-res)]
    (= 409 (:status es-res))))

(s/defn create-policy!
  "Create a lifecycle management policy.
   - For Elasticsearch: Creates an ILM policy
   - For OpenSearch: Creates an ISM policy (automatically transforms ILM if needed)

   The policy parameter should be in ILM format. It will be automatically
   transformed to ISM format if connecting to OpenSearch.

   If the policy already exists (409 conflict), this function will automatically
   update the existing policy using update-policy!."
  [{:keys [uri version engine request-fn] :as conn} :- ESConn
   policy-name :- s/Str
   policy :- Policy]
  ;; Check feature support
  (when-not (feat/lifecycle-management-type conn)
    (throw (ex-info "Lifecycle management not supported"
                    {:engine engine :version version})))

  ;; Transform policy to target engine format
  (let [normalized-policy (normalize-policy policy engine)
        ;; OpenSearch requires the policy in a "policy" wrapper
        request-body (case engine
                       :elasticsearch {:policy policy}
                       :opensearch {:policy normalized-policy}
                       {:policy policy})
        do-create (fn []
                    (-> (make-http-opts conn
                                        {}
                                        []
                                        request-body
                                        nil)
                        (assoc :method :put
                               :url (policy-uri uri policy-name engine))
                        request-fn
                        safe-es-read))]
    (try
      (let [response (do-create)]
        ;; Normalize OpenSearch response to match Elasticsearch format
        (case engine
          :opensearch (if (:_id response)
                        {:acknowledged true}
                        response)
          response))
      (catch Exception e
        (if (policy-conflict? e)
          ;; Policy already exists, update it instead
          (do
            (log/info "Policy" policy-name "already exists, updating...")
            (update-policy! conn policy-name policy))
          ;; Re-throw other exceptions
          (throw e))))))

(s/defn delete-policy!
  "Delete a lifecycle management policy.
   Works with both Elasticsearch ILM and OpenSearch ISM."
  [{:keys [uri version engine request-fn] :as conn} :- ESConn
   policy-name :- s/Str]
  (when-not (feat/lifecycle-management-type conn)
    (throw (ex-info "Lifecycle management not supported"
                    {:engine engine :version version})))
  (let [response (-> (make-http-opts conn)
                     (assoc :method :delete
                            :url (policy-uri uri policy-name engine))
                     request-fn
                     safe-es-read)]
    ;; Normalize OpenSearch response to match Elasticsearch format
    (case engine
      :opensearch (if (= (:result response) "deleted")
                    {:acknowledged true}
                    response)
      response)))

(s/defn get-policy
  "Get a lifecycle management policy.
   Works with both Elasticsearch ILM and OpenSearch ISM.
   Returns the policy in its native format (ILM or ISM)."
  [{:keys [engine] :as conn} :- ESConn
   policy-name :- s/Str]
  (let [response (get-policy-raw conn policy-name)]
    ;; Normalize OpenSearch response to match Elasticsearch format
    ;; ES: {:policy-name {:policy {:phases {...}}}}
    ;; OS GET: {:_id "policy-name" :_seq_no N :_primary_term N :policy {...}}
    ;; We need to wrap the policy in the same structure as Elasticsearch
    (case engine
      :opensearch (when (:_id response)
                    {(keyword policy-name) {:policy (:policy response)}})
      response)))
