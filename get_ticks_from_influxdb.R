library("influxdbr")
library("xts")
library("ggplot2")
library("PerformanceAnalytics")

con <- influx_connection(host = "localhost",)
show_databases(con = con)
result <- influx_select(con = con, 
                        db = "stocks",
                        field_keys = "*",
                        measurement = "option_trades",
                        where = "contractId = '318603987'",
                        order_desc = TRUE, 
                        return_xts = TRUE)
