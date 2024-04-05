package HydraLog::LogRecord;
use strict;
use warnings;
use Carp ();
use Time::Piece ();
use overload '""' => sub { $_[0]->to_string };

=head1 LOG RECORDS

The log records are a blessed hashref of the fields of a log record.  Some fields have
official meaning, but others are just ad-hoc custom fields defined by the user.

All fields can be accessed as attributes, including the ad-hoc custom ones via AUTOLOAD.
Reading the C<timestamp> returns the same integer that was read from the file (but
decoded from hex).  There is a virtual field C<epoch> which scales and adds the timestamp
to the C<start_epoch> of the log file.

=head2 ATTRIBUTES

=over

=item C<to_string>

Not exactly an attribute, this renders the record in "a useful and sensible manner", which
for now means timestamp in local time, level, facility, ident, and message.  It does not
include a trailing newline.

=item C<timestamp>

Unix epoch number, which may contain fractional decimal places  if the timestamps are
sub-second precision.

=item C<timestamp_local>

The timestamp, converted to local time zone in C<< YYYY-MM-DD HH:MM::SS[.x] >> format.

=item C<timestamp_iso8601>

The timestamp, in ISO-8601 C<< YYYY-MM-DDTHH:MM:SS[.x]Z >> format.

=item C<level>

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

=item C<facility>

The syslog facility name.  This is a string, not guaranteed to match any of the constants
on your local C library.

=item C<identity>

The program name that generated the message.  In messages from syslog, this is the
portion before ':' in the logged string.

=item C<message>

The message text.

=item C<*>

Any other field whose name is composed of C<< /\w+/ >> can be accessed as an attribute, but
you get an exception if the field wasn't defined for this log.  This object does not have
any private fields, so you may access C<< ->{$field} >> rather than using an accessor.

=back

=cut

sub import {}
sub DESTROY {}

sub timestamp_epoch { $_[0]{timestamp_epoch} }

sub timestamp_ofs { $_[0]{timestamp_ofs} }

sub timestamp { $_[0]{timestamp} //= $_[0]->_build_timestamp }

sub _build_timestamp {
   $_[0]{timestamp}= Time::Piece->new($_[0]{timestamp_epoch} + $_[0]{timestamp_ofs});
}

sub timestamp_utc {
   my $ts= $_[0]->timestamp or return undef;
   return $ts->date . 'T' . $ts->time . ($ts->epoch =~ /(\.\d+)$/? "$1Z" : 'Z');
}
*timestamp_iso8601= *timestamp_utc;

sub timestamp_local {
   my $ts= $_[0]->timestamp or return undef;
   my $local= Time::Piece::localtime($ts->epoch);
   return $local->date . ' ' . $local->time . ($local->epoch =~ /(\.\d+)$/? $1 : '');
}

sub to_string {
   my $self= shift;
   return join ' ', $self->timestamp_local,
      (defined $self->{level}?     ( $self->level ) : ()),
      (defined $self->{facility}?  ( $self->facility ) : ()),
      (defined $self->{identity}?  ( $self->identity.':' ) : ()),
      (defined $self->{message}?   ( $self->message ) : ());
}

our %_dynamic;
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
   my $name= substr($HydraLog::LogRecord::AUTOLOAD, length(__PACKAGE__)+2);
   exists $_[0]{$name}? $_[0]{$name} : Carp::croak("No field '$name' in record");
}

1;
