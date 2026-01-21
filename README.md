# WAL implementation inspired by InnoDB

- InnoDB Redo Log Design: https://dev.mysql.com/doc/dev/mysql-server/latest/PAGE_INNODB_REDO_LOG_FORMAT.html
- My TLA+ Model: https://github.com/QuangTung97/tla-plus/blob/master/WAL/WAL.tla

I just try to find an algorithm for WAL. And somehow my TLA+ model (independently) specifies roughly how InnoDB
does it, where:

- PageNum in my TLA+ Model => `hdr_no` in InnoDB log block header
- Generation in my TLA+ Model => `epoch_no` in InnoDB log block header (maybe not?)
