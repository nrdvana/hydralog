Hydralog File Format "tsv0"
---------------------------

Hydralog supports multiple formats.  tsv0 is designed to be the simplest of
all of them, for when you want a file format you can operate on without library
assistance.  As the name suggests, tsv0 is built on tab-separated-values.

Features:

  * Common plain text format with almost no formatting requirements
  * Every record can be decoded by splitting on tab characters
  * Supports comments for out-of-band information
  * You can include arbitrary key=value metadata at the start of the file
  * Every record starts with timestamp, level, and message
  * Timestamp is encoded in text-sortable YYYYMMDD notation
  * Supports additional user-defined fields

Limitations:

  * No data may contain any TAB or line-end characters (i.e. no escaping)
  * Changes to the system clock show up in the timestamps
  * Timestamp is verbose
  * No reduction of redundant messages
  * No reduction of common default values for fields

## Specification

### Header

The file is "plain text" with Unix (\x0A) or Windows (\x0D\x0A) line endings.
In the notations below, "(TAB)" represents a literal ASCII 0x09 TAB character.
All other characters are literal, and line ending characters are not shown.

The file starts with a Unix-style directive indicating how to parse the file.
This is not a suggestion to make your log files executable, just matching a
common notation that quickly expresses the idea.

    #!hydralog-dump --format=tsv0

The next comments may contain arbitrary metadata in key=value notation.
The keys may not contain "=" characters, and neither may contain control
characters.

    #% KEY=VALUE(TAB)KEY=VALUE(TAB)...

Before the first record, the columns may be declared as

    #: timestamp(TAB)level(TAB)message(TAB)USER_COLUMN1(TAB)USER_COLUMN2...

The first 3 columns must always be exactly "timestamp", "level", and "message".
Any additional columns are optional and names are chosen by the user.

### Records

Every line of text that does not start with '#' is a log record.  The log data
gets split on TAB characters, and the first three are always the timestamp,
the log-level, and the message.

#### timestamp

The timestamp is always "YYYYMMDD hhmmss[.nnnnn]" in UTC.  The decimal place
in the seconds is optional.

    20240101 000000
    20240101 000000.000

#### level

The log-level is either a case-insensitive full name of a log level (based
on syslog) or single-character abbreviation of the common ones:

      EMERG   EMERGENCY
    A ALERT
    C CRIT    CRITICAL
    E ERR     ERROR
    W WARN    WARNING
    N NOTE    NOTICE
    I INFO
    D DEBUG

You may use additional custom log level names at your own peril.  Tooling must
gracefully handle encountering custom names, but may ignore those records if
log-level filtering is in effect.

#### message

The message is simply a string of text.  Since no control characters are
allowed in the data, multi-line messages must be represented as multiple log
records.  Tooling may attempt to reconstruct multi-line log messages based on
whether a message has identical timestamp to the previous record and a
message beginning with whitespace.

