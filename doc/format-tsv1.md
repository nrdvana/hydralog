Hydralog File Format "tsv1"
---------------------------

Hydralog supports multiple formats.  tsv1 is designed to be a simple human-
readable (ASCII) format that is also a bit smarter and more compact than tsv0.
Where tsv0 tries to eliminate the need for a library to read it, tsv1 just
aims to be easy to port the library to different languages and generally
viewable in a text editor.

Features:

  * Plain-text tab-separated values format
  * Arbitrary key=value metadata for the file as a whole
  * Arbitrary out-of-band information using comments
  * User-defined fields in message records
  * Multi-line messages
  * Optimized encoding of timestamps
  * Can record changes to system clock without breaking message timeline
  * Optimized encoding of default values
  * Optimized encoding of repeat values
  * Comments for out-of-band information

Limitations:

  * Data may not contain control characters
  * Decoding a record requires access to other records
  * Some features require seekable media

## Specification

### Header

The file is "plain text" with Unix (\x0A) or Windows (\x0D\x0A) line endings.
In the notations below, "(TAB)" represents a literal ASCII 0x09 TAB character.
All other characters are literal, and line ending characters are not shown.

The file starts with a Unix-style directive indicating how to parse the file.
This is not a suggestion to make your log files executable, just matching a
common notation that quickly expresses the idea.

    #!hydralog-dump --in-format=tsv1

The next comments may contain arbitrary metadata in key=value notation.
The keys may not contain "=" characters, and neither may contain control
characters.

    #% KEY=VALUE(TAB)KEY=VALUE(TAB)...

Before the first record, the columns may be declared as

    #: NAME:ENCODING=DEFAULT(TAB)...

Where NAME is the logical field name, ENCODING indicates how it is stored, and
DEFAULT is a literal value to use when the field is zero-length.  DEFAULT may
itself be zero-length meaning that the field may have an empty value.

#### Field "dT"

The first field must always be 'dT'.  Timestamps are always integer
seconds unless you specify an encoding of "*N", which means the (fractional)
number of seconds has been multiplied by N before truncating to an integer.

Consider this partial example:

    #% start_epoch=2020-01-01T00:00:00Z
    #: dT:*10
    1       0.1 seconds beyond start_epoch

Timestamps cannot use a DEFAULT value because an empty timestamp is an
indication of a continuation of the previous record.

#### Any text field

Any field containing text may declare an ENCODING of a character set, like
"UTF-8".  Keep in mind the character set may not use any control characters.

### Records

Every line of text that does not start with '#' is a log record, and every
line of text that begins with TAB is a continuation of a previous record.
Aside from thise two special cases, the record is simply a TAB-concatenated
list of field values.

#### Field "dT"

The first field is the timestamp.  Timestamps are encoded as a base64
integer using the alphabet "0-9 A-Z a-z _ -".  The count is an offset from the
previous timestamp unless it begins with an '=' character which means it is an
absolute count from the metadata value "start_epoch".  The count value must
never decrease from line to line.

By default, the integer is in seconds, unless the ENCODING is specified as
"*N" (where N is an integer) to allow additional precision smaller than one
second.  For example, "*1000" means the counts are milliseconds.

Example:

    #% start_epoch=2020-01-01T00:00:00Z
    #: timestamp:*10
    1       0.1 seconds beyond start_epoch
    Z       3.6 seconds beyond start_epoch
    =100    409.6 seconds beyond start_epoch
    5       410.1 seconds beyond start_epoch
            line two of previous message
    3       410.4 seconds beyond start_epoch
    4       410.8 seconds beyond start_epoch

These second counters should always be actual elapsed seconds.  In other
words, where unix-epoch time repeats or skips an integer for leap seconds,
this counter should not.  It should be based on a guaranteed monotonic clock,
if available on the host.  If an implementation is forced to use system time
to generate the running timestamp, discontinuities in the count could result
in buggy behavior of the hydralog utilities when processing the files.

When a field of a record contains multiple lines (this format uses the host's
definition of a line break, rather than specifying it) it gets encoded as a
new line prefixed with a number of TAB characters equal to the current field
position.  (the main benefit being that it lines up in a text editor when
viewing the file)

Consider this example where both the message and a custom user field contain
multiple lines (and rendering tabs as multiple of 8 spaces):

    #: dT   message custom1
    0       Message One
            Line two        Custom line 1
                    Custom line 2
                    Custom line 3
    1       Message Two     Custom Text Two

The logical data contained here is (in JSON)

    { "timestamp":0, "message":"Message One\nLine two",
      "custom1":"Custom line 1\nCustom line 2\nCustom line 3" },
    { "timestamp":1, "message":"Message two", "custom1":"Custom Text Two" },

Obviously it doesn't entirely line up, because tabs are approximated at 8
spaces, but if you loaded it into a tool like Excel as TSV they would line up
in columns.  In a terminal, it is at least fairly readable, moreso if you have
lots of lines per message like would happen in a stack trace.

#### Field "level"

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

#### Field "message"

