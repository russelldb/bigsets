-record(bigset_insert_req_v1, {set :: binary(), %% The name of the set
                               elements :: [binary()] %% to be stored
                              }).
-record(bigset_replicate_req_v1, {set :: binary(),
                                  %% Pairs of binary() element, and
                                  %% it's associated dot.
                                  elements_dots :: {Element :: binary(),
                                                    Dot :: riak_dt_vclock:dot()}
                                 }).
-record(bigset_read_req_v1, {set}).
-record(bigset_contains_req_v1, {set, elements}).
-record(bigset_remove_req_v1, {set, elements, ctx}).

-define(INSERT_REQ, #bigset_insert_req_v1).
-define(REPLICATE_REQ, #bigset_replicate_req_v1).
-define(READ_REQ, #bigset_read_req_v1).
-define(CONTAINS_REQ, #bigset_contains_req_v1).
-define(REMOVE_REQ, #bigset_remove_req_v1).
