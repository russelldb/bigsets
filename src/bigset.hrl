-type delta_element() :: {ElementKey :: binary(),
                          Dot :: riak_dt_vclock:dot()}.

-record(bigset_op_req_v1, {set :: binary(), %% The name of the set
                              inserts:: [binary()], %% to be stored
                              removes :: [{Member :: binary(), Ctx :: binary()}] %% to be removed
                              }).
-record(bigset_replicate_req_v1, {set :: binary(),
                                  inserts :: [delta_element()],
                                  removes :: [{put, K :: binary(), B ::  binary()}]
                                 }).

-record(bigset_read_req_v1, {set}).

-define(OP, #bigset_op_req_v1).
-define(REPLICATE_REQ, #bigset_replicate_req_v1).
-define(READ_REQ, #bigset_read_req_v1).


