package HydraLog::LogReader;
use Moo;
use Carp;
use POSIX 'ceil';

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
	my $line0= $self->_next_line or croak "Can't read first line";
	$line0 =~ /^#!.*?--format=(\w+)/ or croak "Can't parse format from first line";
	$self->_set_format($1);
	if ($self->format eq 'tsv0') {
		my $line1= $self->_next_line or croak "Can't read metadata line";
		my %meta= map {
			/^([^=]+)(?:=(.*))/ or croak "Can't parse metadata line";
			( $1 => ($2 // 1) )
		} split /\t/, substr($line1,2);
		defined $meta{start_epoch} or croak "Metadata doesn't list start_epoch";
		$self->_set_start_epoch(delete $meta{start_epoch});
		$self->_set_timestamp_scale(delete $meta{ts_scale} || 1);
		$self->_set_file_meta(\%meta);
		my $line2= $self->_next_line or croak "Can't read header line";
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
		$self->_index([ [ 0, ($self->fh->tell || croak "tell: $!") ] ]);
		$self->_index_counter($self->autoindex_period);
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

=head2 file_meta

=cut

has filename        => ( is => 'ro' );
has fh              => ( is => 'rwp' );
has 'format'        => ( is => 'rwp' );
has fields          => ( is => 'rwp' );
has field_defaults  => ( is => 'rwp' );
has start_epoch     => ( is => 'rwp' );
has timestamp_scale => ( is => 'rwp' );
has file_meta       => ( is => 'rwp' );
has level_alias     => ( is => 'lazy' );
has _cur_ts         => ( is => 'rw' );
has autoindex_period=> ( is => 'ro', default => 256 );
has autoindex_size  => ( is => 'rw', default => 256 );
has _index          => ( is => 'rw' );
has _index_counter  => ( is => 'rw' );

sub _build_level_alias {
	{
		EM => 'EMERGENCY',
		A  => 'ALERT',
		C  => 'CRITICAL',
		E  => 'ERROR',
		W  => 'WARNING',
		N  => 'NOTICE',
		I  => 'INFO',
		D  => 'DEBUG',
		T  => 'TRACE',
	}
}

has _buffer_read_size => ( is => 'ro',  default => (1<<16) );
has _buffer_file_pos  => ( is => 'rwp', default => 0 );
has _buffer           => ( is => 'rw',  default => '' );
has _need_seek        => ( is => 'rw',  default => 0 );
has _line_ends        => ( is => 'rw',  default => sub{ [] } );
has _cur_line         => ( is => 'rw',  default => -1 );

sub _next_line {
	my $self= shift;
	while ($self->_cur_line >= $#{ $self->_line_ends }) {
		$self->_buffer_grow or return undef;
	}
	$self->_cur_line(my $cur= $self->_cur_line + 1);
	my $start= !$cur? 0 : $self->_line_ends->[$cur-1];
	my $end= $self->_line_ends->[$cur];
	return substr($self->{_buffer}, $start-$self->_buffer_file_pos, $end-$start);
}

sub _prev_line {
	my $self= shift;
	while ($self->_buffer_file_pos > 0 && $self->_cur_line < 1) {
		$self->_buffer_grow_reverse or return undef;
	}
	return undef if $self->_cur_line < 1;
	$self->_cur_line(my $cur= $self->_cur_line - 1);
	my $start= !$cur? 0 : $self->_line_ends->[$cur-1];
	my $end= $self->_line_ends->[$cur];
	return substr($self->{_buffer}, $start-$self->_buffer_file_pos, $end-$start);
}

sub _buffer_grow {
	my $self= shift;
	if ($self->_need_seek) {
		my $goal= $self->_buffer_file_pos + length $self->_buffer;
		my $arrived= sysseek($self->fh, $goal, 0) or croak "seek: $!";
		$arrived == $goal or croak "Seek failed (arrived=$arrived, goal=$goal)";
		$self->_need_seek(0);
	}
	my $got= sysread($self->fh, $self->{_buffer}, $self->_buffer_read_size, length($self->{_buffer}));
	if (!defined $got) {
		# temporary errors?
		return 0 if $!{EINTR} || $!{EAGAIN} || $!{EWOULDBLOCK};
		croak "read: $!";
	} elsif ($got == 0) {
		# EOF, which is permanent on non-seekable media, or maybe temporary on seekable ones.
		$self->_need_seek(1) if $self->seekable;
		return 0;
	} else {
		# Find line-ends in the new section of buffer
		my $pos= length $self->{_buffer} - $got - 1;
		my $ends= $self->_line_ends;
		while (($pos= index($self->{_buffer}, "\n", $pos+1)) >= 0) {
			push @$ends, $pos + $self->_buffer_file_pos;
		}
		return 1;
	}
}
sub _buffer_discard {
	my ($self, $n_bytes)= @_;
	croak "n_bytes > length(buffer)" if $n_bytes > length($self->{_buffer});
	substr($self->{_buffer}, 0, $n_bytes, '');
	my $new_addr= $self->_buffer_file_pos + $n_bytes;
	$self->_buffer_file_pos($new_addr);
	my $ends= $self->_line_ends;
	my $keep= 0;
	while ($keep <= $#$ends && $ends->[$keep] < $new_addr) { $keep++ }
	splice(@$ends, 0, $keep);
	$self->_cur_line($self->cur_line - $keep);
}

sub _buffer_grow_reverse {
	my $self= shift;
	my $read_size= $self->_buffer_read_size;
	$read_size= $self->_buffer_file_pos if $self->_buffer_file_pos < $read_size;
	return 0 if !$read_size;
	# Seek backwards by a read_size before start of buffer
	my $arrived= sysseek($self->fh, $self->_buffer_file_pos - $read_size, 0) or croak "seek: $!";
	$self->_need_seek(1); # position is not at end of buffer anymore
	my $got= sysread($self->fh, my $buf, $read_size);
	if (!defined $got) {
		# temporary errors?  (probably can't happen when reading backward on seekable fh?)
		return 0 if $!{EINTR} || $!{EAGAIN} || $!{EWOULDBLOCK};
		croak "read: $!";
	} elsif ($got != $read_size) {
		# could only happen if something else truncated the file?
		croak "Unexpected partial read while reading backward on handle";
	} else {
		substr($self->{_buffer}, 0, 0, $buf);
		$self->_buffer_file_pos( $self->_buffer_file_pos - $read_size );
		my @ends;
		my $pos= -1;
		while (($pos= index($buf, "\n", $pos+1)) >= 0) {
			push @ends, $pos + $self->_buffer_file_pos;
		}
		splice(@{ $self->_line_ends }, 0, 0, @ends);
		$self->_cur_line($self->cur_line + @ends);
		return 1;
	}
}
sub _buffer_discard_reverse {
	my ($self, $n_bytes)= @_;
	croak "n_bytes > length(buffer)" if $n_bytes > length($self->{_buffer});
	substr($self->{_buffer}, -$n_bytes, $n_bytes, '');
	my $ends= $self->_line_ends;
	my $lim= $self->_buffer_file_pos + length $self->{_buffer};
	$#$ends-- while $ends->[-1] >= $lim;
}

=head1 METHODS

=head2 next

Return the next record from the log file.  The records are lightweight blessed objects that
stringify to a useful human-readable line of text.  See L</LOG RECORDS>.

=head2 peek

Return the next record without advancing the iteration.

=cut

sub next {
	my $self= shift;
	$self->{cur}= defined $self->{next}? delete $self->{next} : $self->_parse_next;
}

sub peek {
	my $self= shift;
	defined $self->{next}? $self->{next} : ($self->{next}= $self->_parse_next);
}

use constant _COMMENT => ord('#');
sub _parse_next {
	my $self= shift;
	my $initial_ts= $self->{_cur_ts};
	defined (my $line= $self->_next_line) or return undef;
	redo if ord $line == _COMMENT || !length $line;
	my $file_pos= $self->{_index_counter}? undef : $self->_line_ends->[$self->_cur_line]+1;
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
	# If it's time for an index entry, add one.  Test was conducted earlier.
	if (defined $file_pos) {
		# but wait, not if the timestamp didn't move since the previous record
		if ($initial_ts < $self->{_cur_ts}) {
			my $index= $self->_index;
			if (@$index >= $self->autoindex_size) {
				# compact index by cutting out every odd element
				$index->[$_]= $index->[$_*2] for 1 .. int($#$index/2);
				$#$index= int(@$index/2)-1;
				$self->autoindex_period($self->autoindex_period * 2);
			}
			push @$index, [ $self->{_cur_ts}, $file_pos ];
		}
	} else {
		--$self->{_index_counter}
	}
	bless \%rec, 'HydraLog::LogReader::Record';
}

=head2 seek

  $reader->seek($goal_epoch);
  $reader->seek($DateTime);

Seek to a unix epoch timestamp within the file so that the next record read is the first
one with a timestamp greater or equal to the goal.  If this seeks beyond the start of the
file the next record will be the first record of the file.  If this seeks beyond the end
of the file the next record will be C<undef>, but you can call L</prev> to get the record
before end of file.

Dies on I/O errors.  Returns C<$reader> for convenience.

=cut

sub seek {
	my ($self, $goal)= @_;
	my $goal_epoch= !ref $goal && Scalar::Util::looks_like_number($goal)? $goal
		: ref($goal) && ref($goal)->can('epoch')? $goal->epoch
		: croak("Can't convert '$goal' to a unix epoch");
	my ($start, $scale)= ($self->start_epoch, $self->timestamp_scale);
	# Is the timestamp before the first record?
	$self->_seek_ts($goal_epoch <= $start? 0 : ceil(($goal_epoch - $start) * $scale));
	$self;
}

sub _seek_ts {
	my ($self, $goal_ts)= @_;
	delete $self->{next};
	my ($cur_ts_ref, $index)= (\$self->{_cur_ts}, $self->_index);
	# If the goal is less or equal to the current pos, use the index and seek backward
	if ($goal_ts <= $$cur_ts_ref) {
		my ($min, $max)= (0, $#$index);
		while ($min < $max) {
			my $mid= int(($max+$min+1)/2);
			if ($goal_ts < $index->[$mid][0]) {
				$max= $mid-1;
			} else {
				$min= $mid;
			}
		}
		$self->fh->seek($index->[$min][1], 0) or croak "seek: $!";
		$self->{_index_counter}= $self->autoindex_period * (@$index - $min);
		$$cur_ts_ref= $index->[$min][0];
	} else {
		# TODO: binary search forward if the file contains index comments
	}
	# Now read forward until the desired record
	# TODO: read blocks and parse the steps without creating record objects
	my $rec;
	while ($$cur_ts_ref < $goal_ts) {
		last unless defined ($rec= $self->_parse_next);
	}
	$self->{next}= $rec;
}

sub seek_last {
	my ($self)= @_;
	# TODO: If the file contains index comments, read blacks backward until the
	# first index point.
	if (0) {
	} else {
		# Are we at the end of the file already?
		my $next= $self->_parse_next;
		if (!$next) {
			# need to rewind to last index point and walk forward again.
			$self->_seek_ts(
		# Else take the last known auto-index and iterate forward.
		if ($self->_index && @{ $self->_index } && $self->{_cur_ts}
	}
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

	sub timestamp_utc {
		my $epoch= $_[0]->timestamp or return undef;
		my ($sec, $min, $hour, $mday, $mon, $year)= gmtime $epoch;
		return sprintf "%04d-%02d-%02dT%02d:%02d:%02d%sZ",
			$year+1900, $mon+1, $mday, $hour, $min, $sec,
			($epoch =~ /(\.\d+)$/? $1 : '');
	}
	*timestamp_iso8601= *timestamp_utc;

	sub timestamp_local {
		my $epoch= $_[0]->timestamp or return undef;
		my ($sec, $min, $hour, $mday, $mon, $year)= localtime $epoch;
		return sprintf "%04d-%02d-%02d %02d:%02d:%02d%s",
			$year+1900, $mon+1, $mday, $hour, $min, $sec,
			($epoch =~ /(\.\d+)$/? $1 : '');
	}

	sub to_string {
		my $self= shift;
		return join ' ',
			(defined $self->{timestamp}? ( $self->timestamp_local ) : ()),
			(defined $self->{level}?     ( $self->level ) : ()),
			(defined $self->{facility}?  ( $self->facility ) : ()),
			(defined $self->{identity}?  ( $self->identity.':' ) : ()),
			(defined $self->{message}?   ( $self->message ) : ());
	}

	sub AUTOLOAD {
		$HydraLog::LogReader::Record::AUTOLOAD =~ /::(\w+)$/;
		return if $1 eq 'DESTROY' || $1 eq 'import';
		Carp::croak("No such field '$1'") unless defined $_[0]{$1};
		my $field= $1;
		no strict 'refs';
		*{"HydraLog::LogReader::Record::$field"}= sub { $_[0]{$field} };
		return $_[0]{$field};
	}
}

1;
