package HydraLog::LogReader;
use Moo;
use Carp;

=head1 SYNOPSIS

  my $reader= HydraLog::LogReader->open("foo.log");
  while (my $record= $reader->next) {
    say $_;
  }
  $reader->close;

=head1 DESCRIPTION

The HydraLog logfiles begin with:

  #!hydralog-dump --format=tsv0
  # start_epoch=$dt	timestamp_scale=1	$name=$value ...
  # timestamp_step_hex=0	level=I	message	CustomField=Y
  $TIMESTEP	$LEVEL	$MESSAGE
  $TIMESTEP	$LEVEL	$MESSAGE
  $TIMESTEP	$LEVEL	$MESSAGE	$CustomField
  $TIMESTEP	$LEVEL	$MESSAGE

In short, this tells us the file format (while self-documenting how to process the file)
various attributes of the file, a list of the fields that will appear in each row,
and then the rows themselves.

The $TIMESTAMP values are hex number of C<< seconds * $ts_scale >> since the start_epoch.

=head1 CONSTRUCTOR

=head2 new

Standard Moo constructor, pass attributes as a hash.

=head2 open

Shortcut for C<< ->new(filename => $name) >>

=cut

sub open {
	my ($class, $filename)= @_;
	$class->new(filename => $filename);
}
sub BUILD {
	my ($self, $args)= @_;
	unless (defined $self->fh) {
		defined $self->filename or croak "You must define either 'filename' or 'fh' attributes";
		CORE::open(my $fh, '<', $self->filename) or croak 'open('.$self->filename."): $!";
		$self->_set_fh($fh);
	}
	my $line0= $self->fh->getline or croak "Can't read first line";
	$line0 =~ /^#!.*?--format=(\w+)/ or croak "Can't parse format from first line";
	$self->_set_format($1);
	if ($self->format eq 'tsv0') {
		my $line1= $self->fh->getline or croak "Can't read metadata line";
		chomp $line1;
		my %meta= map {
			/^([^=]+)(?:=(.*))/ or croak "Can't parse metadata line";
			( $1 => ($2 // 1) )
		} split /\t/, substr($line1,2);
		defined $meta{start_epoch} or croak "Metadata doesn't list start_epoch";
		$self->_set_start_epoch(delete $meta{start_epoch});
		$self->_set_timestamp_scale(delete $meta{ts_scale} || 1);
		$self->_set_custom_attrs(\%meta);
		my $line2= $self->fh->getline or croak "Can't read header line";
		chomp $line2;
		my @fields;
		my %defaults;
		for my $fieldspec (split /\t/, substr($line2,2)) {
			$defaults{$fieldspec}= $1 if $fieldspec =~ s/=(.*)//;
			$fieldspec =~ /^\w+$/ or croak "Invalid field name '$_' in header";
			push @fields, $fieldspec;
		}
		@fields > 0 && $fields[0] eq 'timestamp_step_hex'
			or croak "First column must be timestamp_step_hex";
		$self->_set_fields(\@fields);
		$self->_set_field_defaults(\%defaults);
		$self->_cur_ts(0);
	}
	else {
		croak "Unknown format '".$self->format."'";
	}
}

=head1 ATTRIBUTES

=head2 filename

=head2 fh

=head2 format

=head2 fields

=head2 start_epoch

=head2 ts_scale

=head2 custom_attrs

=cut

has filename        => ( is => 'ro' );
has fh              => ( is => 'rwp' );
has 'format'        => ( is => 'rwp' );
has fields          => ( is => 'rwp' );
has field_defaults  => ( is => 'rwp' );
has start_epoch     => ( is => 'rwp' );
has timestamp_scale => ( is => 'rwp' );
has custom_attrs    => ( is => 'rwp' );
has level_alias     => ( is => 'lazy' );
has _cur_ts         => ( is => 'rw' );

sub _build_level_alias {
	{
		EM => 'EMERGENCY',
		A  => 'ALERT',
		C  => 'CRITICAL',
		E  => 'ERROR',
		W  => 'WARNING',
		N  => 'NOTICE',
		'' => 'INFO',
		D  => 'DEBUG',
		T  => 'TRACE',
	}
}

=head1 METHODS

=head2 next

Return the next record from the log file.  The records are lightweight blessed objects that
stringify to a useful human-readable line of text.  See L</LOG RECORDS>.

=cut

use constant COMMENT => ord('#');
sub next {
	my $self= shift;
	defined (my $line= $self->fh->getline) or return undef;
	chomp $line;
	redo if ord $line == COMMENT || !length $line;
	my %rec;
	@rec{ @{$self->fields} }= split /\t/, $line;
	for (keys %{$self->field_defaults}) {
		$rec{$_}= $self->field_defaults->{$_} unless defined $rec{$_} && length $rec{$_};
	}
	if (defined $rec{timestamp_step_hex}) {
		my $t= ($self->{_cur_ts} += hex $rec{timestamp_step_hex});
		$rec{timestamp}= $t / $self->timestamp_scale + $self->start_epoch;
	}
	my $l;
	$rec{level}= $l
		if defined $rec{level} && defined ($l= $self->level_alias->{$rec{level}});
	bless \%rec, 'HydraLog::LogReader::Record';
}

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

package HydraLog::LogReader::Record {
	use strict;
	use warnings;
	use overload '""' => sub { $_[0]->to_string };

	sub timestamp { $_[0]{timestamp} }
	sub level     { $_[0]{level} }
	sub facility  { $_[0]{facility} }
	sub identity  { $_[0]{identity} }
	sub message   { $_[0]{message} }

	sub timestamp_local {
		my $epoch= $_[0]->timestamp or return undef;
		my ($sec, $min, $hour, $mday, $mon, $year)= localtime $epoch;
		return sprintf "%04d-%02d-%02d %02d:%02d:%02d%s",
			$year+1900, $mon+1, $mday, $hour, $min, $sec,
			($epoch =~ /(\.\d+)$/? $1 : '');
	}

	sub timestamp_iso8601 {
		my $epoch= $_[0]->timestamp or return undef;
		my ($sec, $min, $hour, $mday, $mon, $year)= gmtime $epoch;
		return sprintf "%04d-%02d-%02dT%02d:%02d:%02d%sZ",
			$year+1900, $mon+1, $mday, $hour, $min, $sec,
			($epoch =~ /(\.\d+)$/? $1 : '');
	}

		
	sub to_string {
		my $self= shift;
		return join ' ',
			(defined $self->{timestamp}? ( $self->timestamp_localtime ) : ()),
			(defined $self->{level}?     ( $self->level_name ) : ()),
			(defined $self->{facility}?  ( $self->facility ) : ()),
			(defined $self->{identity}?  ( $self->identity.':' ) : ()),
			(defined $self->{message}?   ( $self->message ) : ());
	}
	
	sub AUTOLOAD {
		$HydraLog::LogReader::Record::AUTOLOAD =~ /::(\w+)$/;
		return if $1 eq 'DESTROY' || $1 eq 'import';
		Carp::croak("No such field $1") unless defined $_[0]{$1};
		my $field= $1;
		no strict 'refs';
		*{"HydraLog::LogReader::Record::$field"}= sub { $_[0]{$field} };
		return $_[0]{$field};
	}
}

1;
