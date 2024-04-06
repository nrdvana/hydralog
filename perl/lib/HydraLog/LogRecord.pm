package HydraLog::LogRecord;
use strict;
use warnings;
use Carp ();
use Time::Piece ();
use overload '""' => sub { $_[0]->to_string };

=head1 DESCRIPTION

LogRecord objects are a simple blessed hashref of the fields of a log record.  Some fields have
official meaning, but others are just ad-hoc custom fields defined by the user.  All fields can
be accessed as attributes, including the ad-hoc custom ones via AUTOLOAD.

Reading the C<timestamp> attribute returns a L<Time::Piece> object.  While Time::Piece accessors
only provide a resolution of seconds, accessing C<< ->epoch >> will give back the original time
number with sub-integer precision if it was available.  The methods L</timestamp_local> and
L</timestamp_utc> also include the fractional seconds.

=head1 CONSTRUCTOR

=head2 new

Create a record from either a list of key/value, or a hashref.  C<timestamp> is required, but
can be a simple number that will get upgraded to a Time::Piece object.   C<message> is strongly
suggested.  C<level> defaults to "INFO" unless specified.  Everything else is optional.

=cut

sub new {
   my $pkg= shift;
   my $self= { @_ == 1 && ref $_[0] eq 'HASH'? %{$_[0]} : @_ };
   defined $self->{timestamp}
      or Carp::croak("timestamp is required (or timestamp_epoch and timestamp_ofs)");
   bless $self, $pkg;
}

=head1 ATTRIBUTES

=head2 timestamp

A Time::Piece object, which may contain fractional decimal places if the timestamps are
sub-second precision.  Use C<< ->timestamp->epoch >> to get the sub-second precision.

=head2 timestamp_local

The timestamp, converted to local time zone in C<< YYYY-MM-DD HH:MM::SS[.x] >> format.

=head2 timestamp_utc

The timestamp, in ISO-8601 C<< YYYY-MM-DDTHH:MM:SS[.x]Z >> format.

=cut

sub timestamp {
   # auto-upgrade scalars into Time::Piece objects
   $_[0]{timestamp}= Time::Piece->gmtime($_[0]{timestamp})
      unless ref $_[0]{timestamp};
   $_[0]{timestamp}
}

sub timestamp_utc {
   my $ts= $_[0]->timestamp;
   return $ts->date . 'T' . $ts->time . ($ts->epoch =~ /(\.\d+)$/? "$1Z" : 'Z');
}

sub timestamp_local {
   my $ts= $_[0]->timestamp;
   my $local= Time::Piece->localtime($ts->epoch);
   return $local->date . ' ' . $local->time . ($ts->epoch =~ /(\.\d+)$/? $1 : '');
}

=head2 level

Log level, which can be any string, but is usually normalized to one of:

  EMERGENCY
  ALERT
  CRITICAL
  ERROR
  WARNING
  NOTICE
  INFO
  DEBUG
  TRACE

This object offers several convenience methods for testing the log level against known values:

=over

=item level_number

  my $n= $record->level_number;

Return a numeric level, using the syslog values where emergency is 0 and trace is 8.
Names like C<< /^debug(\d+)/ >> and C<< /^trace(\d+)/ >> will be given fractions between
above the base value of that level.  Any other unknown value is treated as 'info' (6).

=item level_visible_at

  my $bool= $record->level_visible_at($cutoff_level);

Return true if the record's level_number is less than or equal to the provided C<$cutoff_level>.
C<$cutoff_level> may be a number or symbolic name.

=back

=cut

sub level {
   $_[0]{level} || 'INFO';
}

our %level_alias= (
   '!' => 'EMERGENCY', 'EMERG' => 'EMERGENCY', 'PANIC' => 'EMERGENCY',
   'A' => 'ALERT',
   'C' => 'CRITICAL',  'CRIT' => 'CRITICAL',
   'E' => 'ERROR',     'ERR' => 'ERROR',
   'W' => 'WARNING',   'WARN' => 'WARNING',
   'N' => 'NOTICE',    'NOTE' => 'NOTICE',
   'I' => 'INFO',      '' => 'INFO',
   'D' => 'DEBUG',
   'T' => 'TRACE',
);
our %level_number= (
   EMERGENCY => 0,
   ALERT     => 1,
   CRITICAL  => 2,
   ERROR     => 3,
   WARNING   => 4,
   NOTICE    => 5,
   INFO      => 6,
   DEBUG     => 7,
   TRACE     => 8,
);
$level_number{$_}= $level_number{$level_alias{$_}} for keys %level_alias;

sub _level_number {
   return $_[0] if Scalar::Util::looks_like_number($_[0]);
   my $v= $level_number{uc $_[0]};
   return $v if defined $v;
   return $level_number{uc $1} + sprintf(".%05s", $2) if $_[0] =~ /^(DEBUG|TRACE)(\d+)$/i;
   return $level_number{INFO};
}

sub level_number { _level_number($_[0]->level) }

sub level_visible_at { _level_number($_[0]->level) <= _level_number($_[1]) }

=item C<message>

The message text.

=cut

sub message  { $_[0]{message} }

=item C<*>

Any other field whose name is composed of C<< /\w+/ >> can be accessed as an attribute, but
you get an exception if the field wasn't defined for this log.  This object does not have
any private fields, so you may access C<< ->{$field} >> rather than using an accessor.

=back

=cut

sub to_string {
   my $self= shift;
   return join ' ', $self->timestamp_local,
      (defined $self->{level}?     ( $self->level ) : ()),
      (defined $self->{facility}?  ( $self->{facility} ) : ()),
      (defined $self->{identity}?  ( $self->{identity}.':' ) : ()),
      (defined $self->{message}?   ( $self->message ) : ());
}

# The code below implements dynamic autoloaded methods that only show up with 'can' when the
# underlying hash has attributes for them.  So, $record->can('foo') is true when
# exists($record->{foo}).  It works like normal for non-dynamic methods.

our %_dynamic= ();
sub _mk_accessor {
   my ($pkg, $name)= @_;
   $pkg= ref $pkg || $pkg;
   $_dynamic{$name}= 1;
   eval qq{
      sub ${pkg}::$name {
         exists \$_[0]{$name} or Carp::croak("No field '$name' in record");
         \$_[0]{$name}
      }
      \\&${pkg}::$name;
   } or die $@;
}

sub can {
   return exists $_[0]{$_[1]}? ( $_[0]->SUPER::can($_[1]) || $_[0]->_mk_accessor($_[1]) )
      : $_dynamic{$_[1]}? undef
      : $_[0]->SUPER::can($_[1]);
}

sub AUTOLOAD {
   my $name= substr($HydraLog::LogRecord::AUTOLOAD, 21);
   exists $_[0]{$name}? $_[0]{$name} : Carp::croak("No field '$name' in record");
}
sub import {}  # prevent AUTOLOAD
sub DESTROY {} #

1;
