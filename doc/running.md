# Getting Started

Bigsets is a prototype hacked together by a de-motivated
depressive. If you want to run it today this is what you have to do:

## Erlang

You need erlang, I'm still running Basho old skool r16b2-basho9,
others may work.

## Bigsets repo

Bigsets is super hush-hush secret squirrel so you need to get it from
the basho-bin organization (I'm sure "bin" doesn't mean "trash bin")

    git clone git@github.com:basho-bin/bigsets.git

I assume you have access to basho-bin to read this doc.

Then build

    cd bigsets
    make

This "works on my machine", let me know if you have issues. At this
point maybe check the branches are as I expect. `cd` to deps and check
with `git branch` (the result should at least say `* (detached from
X)` where `X` is:

1. riak_core origin/rdb/exper/bigset
2. eleveldb origin/rdb/streaming-folds
3. `cd c_src/leveldb` rdb/bigset/comparator

You might even chose to make a release or a devrel. From the top-level
cloned "bigsets" directory:

    make rel

or

    make devrel

The former gets you a single node, the latter many. Assuming your
starting small, and ran `make rel`, test if things even work a
little. There are no tests to speak of. Such disgrace.

    rel/bigset/bin/bigset console

if that works, great! Now make a set:

    (bigset@127.0.0.1)1> bigset:make_bigset(<<"test">>, 1000).

The first param is the name of a set, must be a binary, the second the
number of elements in the set. The elements are randomly chosen from
the dictionary file on a mac at `/usr/share/dict/words`. If you don't
have one, the location is set by the macro `WORD_FILE` in the
`src/bigset.erl` modules.

Assuming the response is

    ok

Read the set

    bigset:stream_read(<<"test">>).
    {ok,<<131,108,0,0,0,3,104,2,109,0,0,0,8,107,181,153,204,
      235,151,41,115,97,1,104,2,109,0,...>>,
    780}

The number of results won't necessarily match 1000 from above as we
generate a list of 1000 words and pick from them at random for each
add. Some words are added multiple times. If you want to see what the
result looks like you can try:

    bigset_client:read(<<"test">>, []).
    {ok,{ctx,<<131,108,0,0,0,3,104,2,109,0,0,0,8,107,181,153,
           204,235,151,41,115,97,1,104,2,...>>},
    {elems,[{<<"Aani">>,<<0,0,0,5,0,0,0,1,227>>},
            {<<"Aaronitic">>,<<0,0,0,5,0,0,0,3,113>>},
            {<<"Aaru">>,<<0,0,0,6,0,0,0,2,1,22>>},
            {<<"Abama">>,<<0,0,0,5,0,0,0,3,134>>},
            {<<"Abasgi">>,<<0,0,0,5,0,0,0,3,20>>},
            {<<"Abderian">>,<<0,0,0,5,0,0,0,3,120>>},
            {<<"Abdiel">>,<<0,0,0,5,0,0,0,3,251>>},
            {<<"Abdominales">>,<<0,0,0,5,0,0,0,2,116,0,0,0,6,0,...>>},
            {<<"Abelite">>,<<0,0,0,6,0,0,0,3,1,23>>},
            {<<"Abencerrages">>,<<0,0,0,5,0,0,0,1,46,0,0,0,...>>},
            {<<"Aberdeen">>,<<0,0,0,6,0,0,0,1,1,25,0,...>>},
            {<<"Abies">>,<<0,0,0,5,0,0,0,1,54>>},
            {<<"Abigail">>,<<0,0,0,5,0,0,0,1,212>>},
            {<<"Abipon">>,<<0,0,0,5,0,0,0,3,...>>},
            {<<"Abitibi">>,<<0,0,0,5,0,0,0,...>>},
            {<<"Ablepharus">>,<<0,0,0,5,0,0,...>>},
            {<<"Abnaki">>,<<0,0,0,6,0,...>>},
            {<<"Abner">>,<<0,0,0,5,...>>},
            {<<"Abrahamidae">>,<<0,0,0,...>>},
            {<<"Abrahamite">>,<<0,0,...>>},
            {<<"Abram">>,<<0,...>>},
            {<<"Abra"...>>,<<...>>},
            {<<...>>,...},
            {...}|...]}}

If all that works you probably want to start benchmarking. First make a devrel.

    `make devrel`

## Basho Bench

Clone basho_bench and checkout the bigsets branch:

     git clone git@github.com:basho/basho_bench.git
     git checkout rdb/bigset
     make

Then edit whichever config you want to run:

    ls examples/ | grep set
    bigset.read.config
    bigsets.batch.conc.config
    bigsets.bm.config
    bigsets.conc.config
    bigsets.seq.config
    bigsets.write.config
    riakclient.sets.batch.conc.config
    riakclient.sets.batch.config
    riakclient.sets.bm.config
    riakclient.sets.config
    riakclient.sets.read.config

Let's write some bigsets! Open `bigsets.bm.config`. You'll probably
want to set `{duration, 20}` to something shorter like `{duration, 1}`
for your first run. Likewise, if you're running on a laptop, maybe set
`{concurrent, 50}` down to `{concurrent, 5}`. You'll have to edit
`code_paths` to your `bigsets` build. Probably `{code_paths,
["../bigsets/ebin"]}`. Also you want to change `bigset_nodes` to your
devrel (so `'bigset1@127.0.0.1', 'bigset2@127.0.0.1'` etc) and change
`bigset_mynode` to something like `bigset_bench@127.0.0.1`.

The `key_generator` and `value_generator` set the number of sets and
size of the sets respectively. So by default chose the set name
from 1000 possible, with a pareto distribution, and the element name
from 100,000 possible with uniform distribution. Maybe make
`key_generator`smaller for a short local run.

Now set up your devrel by changing back to the bigsets directory and running `./set-up-dev`

When running `dev/dev1/bin/bigset-admin member-status` shows the
cluster settled so that `pending` is `--` on every line like so:

    ================================= Membership ==================================
    Status     Ring    Pending    Node
    -------------------------------------------------------------------------------
    valid      25.0%      --      'bigset1@127.0.0.1'
    valid      25.0%      --      'bigset2@127.0.0.1'
    valid      25.0%      --      'bigset3@127.0.0.1'
    valid      25.0%      --      'bigset4@127.0.0.1'
    -------------------------------------------------------------------------------
    Valid:4 / Leaving:0 / Exiting:0 / Joining:0 / Down:0

You can run your benchmark:

    cd ../basho_bench
    ./basho_bench examples/bigsets.bm.config

When it's over, run `make results` and open `tests/current/summary.png`. Note that if you get a make error stating "Rscript: No such file or directory", then you need to install the R statistics language; see the Basho Bench [Prerequisites](http://docs.basho.com/riak/latest/ops/building/benchmarking/#Generating-Benchmark-Graphs) for generating benchmark graphs.

All that remains is to set up a Riak 2.1 cluster devrel, and edit the
`riakclient.sets.bm.config` to be equivalent to the
`bigsets.bm.config` and run a comparative benchmark. This is left as
an exercise for the reader (NOTE: you will need to create a
bucket-type of datatype=set called <<"sets">>)



