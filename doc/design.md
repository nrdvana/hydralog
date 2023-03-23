Hydralog
--------

Hydralog is a multi-headed log processor.  It reads logs from one or more pipes,
sockets, fifos, or kernel.  Rather than directly processing the logs, it writes
them to text files (which you can put in a tmpfs if you like) and then processors
of your choice can request an event stream about which log files matching your
criteria are available and which have activity.

Hydralog is a single threaded non-blocking state machine that does nothing more
than copy buffers and queue some notifications, so it is extremely efficient.
But, because it is a single thread, it could possibly fall behind if you have
many processes generating data faster than a single CPU core can consume it.

Hydralog does not manage or monitor the log processor programs, so it is up to
you to configure and monitor your processors via other means, like a process
supervisor.

One of Hydralog's more unique features is the ability to serve out pipe handles
over a Unix socket, so that the program producing the logs can write directly
to the pipe without another process involved.

Algorithms
----------

### Startup

  - Read command line options, and config file, and merge to create
    list of channel objects and syslog mappings.
  - Get the current epoch time and monotonic time
  - For each channel, see if the current monotonic clock vs. system clock
    agrees with the difference between monotonic and system clock in the
    file.  If not, rotate the channel log file.

### Main Loop

  - Read the monotonic clock
  - For each pipe
    - Read into pipe's buffer.
    - For each complete message
      - Write to the channel's buffer (timestamped)
      - If destination is full, flush buffer (see below)
  - For the syslog socket,
    - For each datagram
      - Identify the channel for this facility+ident
        - If the exact ident+facility is found in the hash table, use that
        - else iterate through the regexes for a match, and cache it if room,
        - else log to the channel for the facility itself.
      - Write to the channel's buffer (timestamped)
      - If destination is full, flush buffer (see below)
  - For each channel, flush to disk
  - For each control socket
    - Process each command
      (list/create channels, list/create syslog patterns, create pipes, etc)
  - Sleep until activity on input pipe

### Shutdown

  - On receipt of first sigterm, hydralog begins rejecting any command that
    would create additional logging, and if all incoming pipes are closed,
    it will proceed to close all control sockets and exit.  Until then it
    continues to report channel progress to all listening control sockets.
  - On receipt of a second sigterm when input pipes remain, hydralog forks
    off a child process for each remaining input pipe to deliver it to the
    file, and then exits.
  - On receipt of SIGQUIT or SIGSEGV, hydralog forks off a child process for
    each remaining input pipe to deliver it to the file, and then exits.

### Command: new_pipe

  - Allocate a new Input record, with attributes specified by caller
  - Create a unix pipe
  - Set read end to nonblocking.
  - If all success, deliver write end over the Unix socket to the requestor
  - else report failure and clean up

### Command: recv_pipe

  - Allocate a new Input record, with attributes specified by caller
  - Store the received pipe handle in it
  - Set to nonblocking
  - Respond success or failure.

Objects
-------

### LogFile

  - filename: relative to channel directory
  - logfd: file handle
  - version: hydralog version that wrote this file
  - format: enum of different encodings for the file data
  - meta_const: list of name+value from the header of the log
  - fields: array of fields which appear on each record of the file
  - start_epoch: unix epoch when log file was created
  - start_ts: monotonic clock value when log file was created
  - last_ts: monotonic value of final record
  - lim_ts: monotonic value when log file must be rotated next
  - lim_size: upper size limit for log file

### Channel

  - path: directory name, relative to CWD of hydralog
  - format: requested format for new log files
  - fields: requested fields for new log files
  - rotate_cal: enum(hour, day, month) of what calendar boundaries to rotate the file
  - rotate_size: max bytes before rotating file
  - current_log: reference to LogFile being written

### SourcePipe

  - channel: reference to Channel destination
  - constants: list of field and value which will be applied to all log records
  - in_fd: pipe read-end filehandle

### SourceSyslog

  - path: unix domain socket path
  - in_fd: socket file descriptor
  - ident_map: hash table of literal idents to an array of channel-per-facility
  - ident_pattern: array of (regex_t, facilities, channel)

### SourceKmsg

  - channel
  
