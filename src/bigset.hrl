-type set() :: binary().
-type member() :: binary().
-type actor() :: binary().
-type counter() :: pos_integer().
-type key() :: binary().
-type decoded_key() :: clock_key() | element_key() | end_key().
-type clock_key() :: {clock, set(), actor()}.
-type end_key() :: {end_key, set()}.
-type element_key() :: {element, set(), member(), actor(), non_neg_integer()}.
-type dot() :: bigset_clock:dot().
-type dot_list() :: [dot()].
-type ctx() :: binary().
-type add() :: {member(), ctx()} | member().
-type adds() :: {member(), ctx()}.
-type remove() :: {member(), ctx()} | member().
-type removes() :: [remove()].
-type db() :: eleveldb:db_ref().

-type delta_add() :: {ElementKey :: binary(),
                          dot_list()}.
-type delta_remove() :: {member(), dot_list()}.

-record(bigset_read_fsm_v1, {req_id,
                             from,
                             set,
                             options}).

-record(bigset_query_fsm_v1, {req_id,
                              from,
                              set,
                              members,
                              options}).

-record(bigset_op_req_v1, {set :: binary(), %% The name of the set
                           inserts:: adds(), %% to be stored
                           %% to be removed, require a per element ctx at present
                           removes :: removes(),
                           %% dictionary of actor->index mappings for
                           %% the per element ctx The aim here is to
                           %% not send big actor IDs when a single
                           %% small integer will do. This is a
                           %% dictionary for the a simple dictionary
                           %% coder compresssion of actor ids, the
                           %% `Ctx' in `removes' above is compressed
                           %% with this dictionary.
                           ctx :: binary()
                          }).
-record(bigset_replicate_req_v1, {set :: set(),
                                  inserts :: [delta_add()],
                                  removes :: [delta_remove()]
                                 }).

-record(bigset_read_req_v1, {set}).

-record(bigset_contains_req_v1, {set :: set(), %% The set
                                 members :: [member()] %%  elements to check membership
                                }).

%% Tombstone byte meaning
-define(ADD, $a).
-define(REM, $r).

-define(OP, #bigset_op_req_v1).
-define(REPLICATE_REQ, #bigset_replicate_req_v1).
-define(READ_REQ, #bigset_read_req_v1).
-define(CONTAINS_REQ, #bigset_contains_req_v1).

-define(READ_FSM_ARGS, #bigset_read_fsm_v1).
-define(QUERY_FSM_ARGS, #bigset_query_fsm_v1).

-define(DEFAULT_BATCH_SIZE, 1000).
-define(DEFAULT_WORKER_POOL, 100).

-define(READ_OPTS, [{fill_cache, true}]).
-define(WRITE_OPTS, [{sync, false}]).
-define(FOLD_OPTS, [{iterator_refresh, true}]).
