#pragma D option quiet
#pragma D option aggsize=16m
#pragma D option bufsize=32m
#pragma D option dynvarsize=64m

uint64_t  p[string, string, int];

inline int EXPECTED_MAX = 300000;
inline int BUCKET_SIZE = 1000;

/* See bigset_dtrace.hrl for int tags */

/* Process entered measured section. */
erlang$target:::user_trace-i4s4
/arg2 == 1 /
{
  this->proc = copyinstr(arg0);
  this->tag = arg3;
  p[this->proc, "start", this->tag] = timestamp;
}

/* Process exits measured section */
erlang$target:::user_trace-i4s4
/ arg2 == 2 && p[copyinstr(arg0), "start", arg3] != 0/
{
  this->proc = copyinstr(arg0);
  this->tag = arg3;
  this->start = p[this->proc, "start", this->tag];
  this->elapsed = (timestamp - this->start) / 1000;

  p[this->proc, "start", this->tag] = 0;
  p[this->proc, "elapsed", this->tag] = 0;

  @lmin = min(this->elapsed);
  @lmax = max(this->elapsed);
  @lavg = avg(this->elapsed);
  @lsum = sum(this->elapsed);
  @lq = lquantize(this->elapsed, 0, EXPECTED_MAX, BUCKET_SIZE);

  @cnt = count();
}

BEGIN {
  printf("%10s, %10s, %10s, %10s, %10s\n", "Count", "Min", "Avg", "Max", "Total");
}

profile:::tick-1s
{

   /* trunc(@lmin); trunc(@lmax); trunc(@lavg); */
   /* trunc(@cnt); */
}

profile:::tick-10s
{
  printa(@lq);
  trunc(@lq);
}
END
{
  printf("%10s, %10s, %10s, %10s, %10s\n", "Count", "Min", "Avg", "Max", "Total");
  printa("%@10u, %@10u, %@10u, %@10u, %@10u\n", @cnt, @lmin , @lavg , @lmax, @lsum);
  printf("done\n");
}
