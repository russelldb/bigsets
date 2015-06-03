#!/usr/bin/env Rscript

library(ggplot2)
library(getopt)

# Setup parameters for the script
params = matrix(c(
  'indir',   'i', 2, "character",
  'set',     's', 2, "character",
  'ylog',    'l', 0, "logical"
  ), ncol=4, byrow=TRUE)


# Parse the parameters
opt = getopt(params)

plot_data <- function(opt, infile, outfile) {

   writes <- read.csv(infile, header=TRUE, sep=",")
   qp <- qplot(Elements, Time, data=writes, color=Type, main="Type")
   qp <- qp + ylab("MicroSeconds") + xlab("Elements")

   if(!is.null(opt$ylog))
    {
      qp <- qp + scale_y_log10()
    }

   ggsave(outfile, plot=qp, width=10)

}

out_file <- function(opt, type)
   {
      if(!is.null(opt$ylog))
         {
             return(file.path(opt$indir, paste(paste(opt$set, type, "ylog",  sep="_"), "png", sep=".")))
         } else {
             return(file.path(opt$indir, paste(paste(opt$set, type, sep="_"), "png", sep=".")))
         }
   }

writes_file <- file.path(opt$indir, paste(opt$set, "writes", sep="."))
reads_file <- file.path(opt$indir, paste(opt$set, "reads", sep="."))
writes_out  <- out_file(opt, "writes")
reads_out  <- out_file(opt, "reads")

options(scipen=1000000)

plot_data(opt, reads_file, reads_out)
plot_data(opt, writes_file, writes_out)




dev.off()









