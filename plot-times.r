library(ggplot2)
ten <- read.csv("dt-vs-bs.csv", header=TRUE, sep=",")
qp <- qplot(Elements, Time, data=ten, color=Type, main="Type")
qp <- qp + ylab("MicroSeconds") + xlab("Elements")
qp <- qp + scale_y_log10()
ggsave("dt-vs-bs-ylog.png", plot=qp, width=10)
dev.off()









