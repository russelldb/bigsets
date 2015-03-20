-type delta_element() :: {Element :: binary(),
                          Dot :: riak_dt_vclock:dot()}.

-record(bigset_op_req_v1, {set :: binary(), %% The name of the set
                              inserts:: [binary()], %% to be stored
                              removes :: [binary()], %% to be removed
                              ctx :: riak_dt_vclock:vclock()
                              }).
-record(bigset_replicate_req_v1, {set :: binary(),
                                  %% Pairs of binary() element, and
                                  %% it's associated dot.
                                  delta_elements :: [delta_element()]
                                 }).

-record(bigset_read_req_v1, {set}).
-record(bigset_contains_req_v1, {set, elements}).
-record(bigset_remove_req_v1, {set, elements, ctx}).

-define(OP, #bigset_op_req_v1).
-define(REPLICATE_REQ, #bigset_replicate_req_v1).
-define(READ_REQ, #bigset_read_req_v1).
-define(CONTAINS_REQ, #bigset_contains_req_v1).
-define(REMOVE_REQ, #bigset_remove_req_v1).

