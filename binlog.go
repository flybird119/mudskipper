package main

import (
  "os"
  "fmt"
  "log"
  "os/exec"
  "bufio"
  "regexp"
  "strings"
  "path/filepath"
  "time"
  "io"
)

type BinlogReader struct {
  LineBuffer *bufio.Reader
  cmd *exec.Cmd
}

func (L *BinlogReader) ReadString (delim byte) (line string, err error) {
  return L.LineBuffer.ReadString(delim)
}

func (L *BinlogReader) Open (Executable string, BinlogPath string, BaseName string, BinlogNumber int) (bool) {

  var BinlogIndex string = BinlogPath + BaseName + ".index"
  var BinlogFileName string

  BinlogFileName = fmt.Sprintf("%s%s.%06d", BinlogPath, BaseName, BinlogNumber)

  FilePart := fmt.Sprintf("%s.%06d", BaseName, BinlogNumber)
  if FilePart == getActiveBinlog(BinlogIndex) {
    // We don't want to work with the active binlog.
    return false
  }

  for { // Look for our file before we attempt to open it.
    if _, err := os.Stat(BinlogFileName); err != nil {
      if os.IsNotExist(err) {
        log.Println("File does not exist: ", BinlogFileName, "Sleeping for 30 minutes.")
        time.Sleep(1800000 * time.Millisecond)
      } else {
        log.Fatal("Couldn't stat ", BinlogFileName, " Error is ", err)
      }
    } else {
      break
    }
  }

  log.Println("Opening binlog ", BinlogFileName)
  L.cmd = exec.Command(Executable, "--base64-output=decode-rows", "-v", BinlogFileName)
  stdout, err := L.cmd.StdoutPipe()
  if err != nil {
    log.Fatal("Couldn't set up stdout pipe to mysqlbinlog. Failed with error ", err)
  }
  if err := L.cmd.Start(); err != nil {
    log.Fatal("Couldn't start mysqlbinlog. Failed with error ", err)
  }
  L.LineBuffer = bufio.NewReader(stdout)
  return true
}

func (L *BinlogReader) Close () {
  if err := L.cmd.Wait(); err != nil {
    log.Fatal("Couldn't call Wait() on binlogreader. Error is ", err)
  }
}

type msg struct {
  msg string
}

func (e msg) Error() string {
  return e.msg
}

type Row struct {
  fields []string
  Operation string
  BinlogNumber int
  BinlogDate string
}

func Scan (table string, BinlogNumber int, DstDir string, Executable string, BinlogPath string, BaseName string, workers chan int, currentBinlog chan bool) {

  var BeforeImg []string
  var AfterImg []string
  var BinlogDate string
  var RawLine string
  var err error

  Binlog := new(BinlogReader)
  status := Binlog.Open(Executable, BinlogPath, BaseName, BinlogNumber)
  if status == false {
    <-workers // Turn in our work ticket. Free up our slot.
    currentBinlog <- true
    return
  }
  currentBinlog <- false

  rows := make(chan Row)
  go writeLoaderFiles(table, DstDir, rows)

  SchemaAndTable := strings.Split(table, ".")
  ReTableMap, _ := regexp.Compile("Table_map: `" + SchemaAndTable[0] + "`.`" + SchemaAndTable[1] + "`")
  ReSTMT, _ := regexp.Compile("STMT_END_F")
  ReInsert, _ := regexp.Compile("### INSERT INTO")
  ReUpdate, _ := regexp.Compile("### UPDATE")
  ReDeleteFrom, _ := regexp.Compile("### DELETE FROM")
  ReDateFields, _ := regexp.Compile("^#([0-9][0-9])([0-9][0-9])([0-9][0-9]) ")

L1:  for {
    // Eat lines til we hit something we're interested in.
    for ; !ReTableMap.MatchString(RawLine); {
      RawLine, err = Binlog.ReadString('\n')
      if err != nil {
       break L1
      }
    }
    DateFields := ReDateFields.FindStringSubmatch(RawLine)
    BinlogDate = "20" + DateFields[1] + "-" + DateFields[2] + "-" + DateFields[3]

    // Advance to the STMT_END_F line.
    for ; !ReSTMT.MatchString(RawLine); {
      RawLine, err = Binlog.ReadString('\n')
      if err != nil {
        break L1
      }
    }
    // This line tells us ins|upd|delete
    RawLine, err = Binlog.ReadString('\n')
    if err != nil {
      break L1
    }

    // eat the WHERE or the SET
    _, err = Binlog.ReadString('\n')
    if err != nil {
      break L1
    }

    switch {
      case ReDeleteFrom.MatchString(RawLine) :
        BeforeImg, err = getFields(Binlog.LineBuffer)
        rows <- Row{BeforeImg, "DELETE", BinlogNumber, BinlogDate}
      case ReInsert.MatchString(RawLine) :
        AfterImg, err = getFields(Binlog.LineBuffer);
        rows <- Row{AfterImg, "INSERT", BinlogNumber, BinlogDate}
      case ReUpdate.MatchString(RawLine) :
        BeforeImg, err = getFields(Binlog.LineBuffer)
        if err == nil {
          AfterImg, err = getFields(Binlog.LineBuffer)
        }
        rows <- Row{AfterImg, "UPDATE", BinlogNumber, BinlogDate}
    }
  }
  if (err != nil) && (err != io.EOF) {
    msg := fmt.Sprintf("Something went wrong with binlog number %d. The error is %s.", BinlogNumber, err)
    log.Println(msg)
  }
  Binlog.Close()
  close(rows)
  <-workers // Turn in our work ticket.
}

// Return an array representing a changed row.
func getFields (LineBuffer *bufio.Reader) (fields []string, err error) {

  ReCol, _ := regexp.Compile("###   @[0-9]+=(.*)")

  for {
    RawCol, err := LineBuffer.ReadString('\n')
    if err != nil {
      return fields, err
    }
    if !ReCol.MatchString(RawCol) {
      break
    }
    match := ReCol.FindStringSubmatch(RawCol)
    if len(match) != 2 {
      return fields, &msg{"Missing column"}
    }
    extracted_field := match[1]
    fields = append(fields, extracted_field)
  }
  return fields, nil
}

func writeLoaderFiles (Table string, DstDir string, rows chan Row) {

  var BinlogNumber int = 0
  var BinlogDate string = "0000-00-00"
  var d string
  var FileFQN string
  var FinalFileFQN string
  var OurFile *os.File
  var err error
  var j int
  var FieldCount int
  var first_pass bool = true

  for i := range rows {
    if first_pass == true {
      // Although a binlog can straddle mulitple days, we take the first date that we find.
      BinlogDate = i.BinlogDate
      BinlogNumber = i.BinlogNumber
      FileFQN = fmt.Sprintf("%s/%s-%06d.txt.in_progress", DstDir, BinlogDate , BinlogNumber)
      FinalFileFQN = fmt.Sprintf("%s/%s-%06d.txt", DstDir, BinlogDate , BinlogNumber)
      OurFile, err = os.Create(FileFQN)
      if err != nil {
        log.Fatal("Couldn't create ", FileFQN, " Error is ", err)
      }
      log.Println("Created ", FileFQN)
      first_pass = false
    }
    // Assemble and write the row
    if FieldCount = len (i.fields); FieldCount > 0 {
      d = fmt.Sprintf("%s\t%s\t", i.BinlogDate, i.Operation)
      for j = 0; j <  FieldCount - 1; j++ {
        d += fmt.Sprintf("%s\t", i.fields[j])
      }
      d += fmt.Sprintf("%s\n", i.fields[j])
      _, err := OurFile.WriteString(d)
      if err != nil {
        log.Fatal("Couldn't write to ", FileFQN, " Error is ", err)
      }
    } else {
      log.Println("Ignoring row with zero fields.")
    }
  }
  if first_pass == false {
    err = OurFile.Close()
    if err != nil {
      log.Fatal("Couldn't close file ", FileFQN, " Error is ", err)
    }
    err = os.Rename(FileFQN, FinalFileFQN)
    if err != nil {
      log.Fatal("Couldn't rename file ", FileFQN, " Error is ", err)
    }
  }
}

func getActiveBinlog (BinlogIndex string) (string) {
  out, _ := exec.Command("/usr/bin/tail", BinlogIndex, "-n", "2", "|", "head", "-n", "1").Output()
  FilePart := filepath.Base(string(out[:]))
  return strings.Trim(FilePart, "\n")
}
