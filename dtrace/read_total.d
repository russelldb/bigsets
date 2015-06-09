#pragma D option quiet
int p[string,string, int];

/* See bigset_dtrace.hrl for string tags */

/* Process entered measured section. */
erlang$target:::user_trace-i4s4
/arg2 == 1 /
{
  this->proc = copyinstr(arg0);
  this->tag = arg3;
  p[this->proc, "start", this->tag] = timestamp;
  p[this->proc, "vstart", this->tag] = vtimestamp;
}

/* Process exits measured section */
erlang$target:::user_trace-i4s4
/arg2 == 2 && p[copyinstr(arg0), "start", arg3] != 0 /
{
  this->proc = copyinstr(arg0);
  this->tag = arg3;
  this->start = p[this->proc, "start", this->tag];
  this->elapsed = (timestamp - this->start) / 1000;

  this->vstart = p[this->proc, "vstart", this->tag];
  this->rolling_elapsed = p[this->proc, "velapsed", this->tag];
  this->velapsed = ((vtimestamp - this->vstart) + this->rolling_elapsed) / 1000;

  p[this->proc, "start", this->tag] = 0;
  p[this->proc, "elapsed", this->tag] = 0;

  p[this->proc, "vstart", this->tag] = 0;
  p[this->proc, "velapsed", this->tag] = 0;

  @lmin = min(this->elapsed);
  @lmax = max(this->elapsed);
  @lavg = avg(this->elapsed);

  @vlmin = min(this->velapsed);
  @vlmax = max(this->velapsed);
  @vlavg = avg(this->velapsed);

  @cnt = count();
}

/* If process running our section is unscheduled, remember where we were */
erlang$target:::process-unscheduled
/ p[copyinstr(arg0), "start", arg3] != 0 /
  {
      this->proc = copyinstr(arg0);
      this->tag = arg3;
      this->vstart = p[this->proc, "vstart", this->tag];
      p[this->proc, "velapsed", this->tag] += (vtimestamp - this->vstart);
      @unscheduled = count();
  }

/* If process that was interrupted in our section runs again */
erlang$target:::process-scheduled
/ p[copyinstr(arg0), "start", arg3] /
  {
      this->proc = copyinstr(arg0);
      this->tag = arg3;
      p[this->proc, "vstart", this->tag] = vtimestamp;
  }

BEGIN {
    printf("%10s, %10s, %10s, %10s\n", "Count", "Min", "Avg", "Max");
}

profile:::tick-1s
{
   printa("%@10u, %@10u, %@10u, %@10u\n", @cnt, @lmin , @lavg , @lmax);
   printa("%@10u, %@10u, %@10u, %@10u\n", @unscheduled, @vlmin , @vlavg , @vlmax);
   /* trunc(@lmin); trunc(@lmax); trunc(@lavg); */
   /* trunc(@cnt); */
}

END
{
  printf("done\n");
}
