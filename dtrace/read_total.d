#pragma D option quiet
int p[string,string];

/* Process entered measured section. */
erlang$target:::user_trace-i4s4
/arg2 == 1/
{
  this->proc = copyinstr(arg0);
  p[this->proc, "start"] = timestamp;
  p[this->proc, "elapsed"] = 0;
}
/* Process exits measured section */
erlang$target:::user_trace-i4s4
/arg2 == 2 && p[copyinstr(arg0), "start"] != 0/
{
  /* turn nanos to micros */
  this->proc = copyinstr(arg0);
  this->start = p[this->proc, "start"];
  this->rolling_elapsed = p[this->proc, "elapsed"];
  this->elapsed = ((timestamp - this->start) + this->rolling_elapsed) / 1000;


  @lmin = min(this->elapsed);
  @lmax = max(this->elapsed);
  @lavg = avg(this->elapsed);
  @cnt = count();

  p[this->proc, "start"] = 0;
  p[this->proc, "elapsed"] = 0;


}


/* If process running our section is unscheduled, remember where we were */
 /* erlang$target:::process-unscheduled */
/*  / p[copyinstr(arg0), "start"] != 0 / */
/*  { */
/*      this->proc = copyinstr(arg0); */
/*      this->start = p[this->proc, "start"]; */
/*      p[this->proc, "elapsed"] += (timestamp - this->start); */
/*      @unscheduled = count(); */
/*  } */

/* /\* /\\* If process that was interrupted in our section runs again *\\/ *\/ */
/*  erlang$target:::process-scheduled */
/*  / p[copyinstr(arg0), "start"] / */
/*  { */
/*      this->proc = copyinstr(arg0); */
/*      p[this->proc, "start"] = timestamp; */
/*  } */

BEGIN {
    printf("%10s, %10s, %10s, %10s\n", "Count", "Min", "Avg", "Max");
}

profile:::tick-1s
{
  printf("%10s, %10s, %10s, %10s\n", "Count", "Min", "Avg", "Max");
   printa("%@10u, %@10u, %@10u, %@10u\n", @cnt, @lmin , @lavg , @lmax);

   /* trunc(@lmin); trunc(@lmax); trunc(@lavg); */
   /* trunc(@cnt); */
}

END
{
  printf("done\n");
}
