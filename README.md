# M U D S K I P P E R

Want to capture row-based changes and store them in an easy-to-use format?
Need to do it without having to write and maintain triggers?
Well, Now you can with Mudskipper!

MySQL row-based replication affords the opportunity to see row-level changes to any table.
The mysqlbinlog cmd line tool bundled with mysql provides a way to parse these changes out of binlogs.
Leveraging the replication behavior and the parsing utility, it's possible to capture change data history.

How it works:
Turn on binary logging and row-based replication.
Mudskipper watches for newly-created MySQL binlogs.
When it finds one, it uses mysqlbinlog to scan the binary log for relevant changes.
It writes the changes it finds (INSERT, UPDATE, DELETE)  to tab-delimited text files.

This project is written in Go.

    cd $GOPATH/src
    git clone git://github.com/lookout/mudskipper mudskipper
    cd mudskipper
    go install

The Makefile builds a Debian package.
