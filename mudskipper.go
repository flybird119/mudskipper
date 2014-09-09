package main

import (
  "runtime"
  "log"
  "time"
  "flag"
)

// The cmd line args
var BinlogNumber = 1000
var TableName string = "flexd_production.memberships"
var BaseName string = "mysql-bin"
var DstDir string = "/tmp"
var BinlogPath string = "/var/lib/mysql"
var Executable string = "/usr/bin/mysqlbinlog"

func init() {
  flag.IntVar(&BinlogNumber, "binlog_number", 1017, "MySQL binlog to start processing.")
  flag.StringVar(&TableName, "table", "flexd_production.memberships", "Fully qualified table name.")
  flag.StringVar(&BaseName, "base_name", "mysql-bin", "Base name for the binlog files, eg mysql-bin")
  flag.StringVar(&DstDir, "dest_dir", "/tmp", "Where to write the output.")
  flag.StringVar(&BinlogPath, "binlog_path", "/var/lib/mysql", "Where the binlogs live.")
  flag.StringVar(&Executable, "executable", "/usr/bin/mysqlbinlog", "Where the mysqlbinlog executable lives.")
  flag.Parse()
}


func main() {

  runtime.GOMAXPROCS(8)
  workers := make(chan int, 8)
  currentBinlog := make(chan bool, 1)

  for {
    workers <- 1 // Hand out a work ticket.
    go Scan(TableName, BinlogNumber, DstDir, Executable, BinlogPath, BaseName, workers, currentBinlog)
    if <-currentBinlog {
      log.Println("We are up to the active binlog. Sleeping for 5 minutes.")
      time.Sleep( 300000 * time.Millisecond)
    } else {
      BinlogNumber += 1
    }
  }
}
