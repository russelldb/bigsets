#pragma D option quiet
#pragma D option aggsize=8m
#pragma D option bufsize=16m
#pragma D option dynvarsize=16m
#pragma D option aggrate=0
#pragma D option cleanrate=50Hz

uint64_t  p[string, string, int];

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

  @cnt = count();
}

BEGIN {
    printf("%10s, %10s, %10s, %10s\n", "Count", "Min", "Avg", "Max");
}

profile:::tick-1s
{
   printa("%@10u, %@10u, %@10u, %@10u\n", @cnt, @lmin , @lavg , @lmax);

   /* trunc(@lmin); trunc(@lmax); trunc(@lavg); */
   /* trunc(@cnt); */
}

END
{
  printf("done\n");
}
