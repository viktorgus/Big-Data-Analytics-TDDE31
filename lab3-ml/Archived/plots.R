library("ggplot2")

dat = read.csv2("lab 3 results csv.txt", sep = ";", header = FALSE)

x= seq(4,24,2)
f = data.frame(
  time = x,
  sum = as.numeric(as.character(dat$V2)),
  factor = as.numeric(as.character(dat$V3))
)



ggplot(data = f, aes(x=time, y=factor,color="Factor")) + geom_line() + geom_line(data=f, aes(x=time, y=sum,color="Sum"))+ylab("temperature") 
