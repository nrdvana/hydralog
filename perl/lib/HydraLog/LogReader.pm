package HydraLog::LogReader;
use Moo;
use Carp;
use POSIX 'ceil';
use HydraLog::StreamLineIter;

=head1 SYNOPSIS

  my $reader= HydraLog::LogReader->open("foo.log");
  while (my $record= $reader->next) {
    say $_;
  }
  $reader->close;

=head1 DESCRIPTION

This object reads Hydralog log files.  It supports various formats, and can detect the format
from a supplied file handle.  It can also seek on a file to a requested timestamp, assuming the
file was written with correctly monotonic timestamps.

=head1 CONSTRUCTOR

=head2 new

Standard Moo constructor, pass attributes as a hash.

=head2 open

Shortcut for C<< ->new(filename => $name) >>

=cut

sub open {
   my ($class, $filename, %options)= @_;
   $class->new(filename => $filename, %options);
}
sub BUILD {
	my ($self, $args)= @_;
	unless (defined $self->fh) {
		defined $self->filename or croak "You must define either 'filename' or 'fh' attributes";
		CORE::open(my $fh, '<', $self->filename) or croak 'open('.$self->filename."): $!";
		$self->_set_fh($fh);
	}
	$self->_line_iter(HydraLog::StreamLineIter->new(fh => $self->fh));
	my $line0= $self->_line_iter->next or croak "Can't read first line";
	$line0 =~ /^#!.*?--format=(\w+)/ or croak "Can't parse format from first line";
	$self->_set_format($1);
	if ($self->format eq 'tsv0') {
		my $line1= $self->_line_iter->next or croak "Can't read metadata line";
		my %meta= map {
			/^([^=]+)(?:=(.*))/ or croak "Can't parse metadata line";
			( $1 => ($2 // 1) )
		} split /\t/, substr($line1,2);
		defined $meta{start_epoch} or croak "Metadata doesn't list start_epoch";
		$self->_set_start_epoch(delete $meta{start_epoch});
		$self->_set_timestamp_scale(delete $meta{ts_scale} || 1);
		$self->_set_file_meta(\%meta);
		my $line2= $self->_line_iter->next or croak "Can't read header line";
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
		$self->_index([ [ 0, $self->_line_iter->next_line_addr ] ]);
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

has _line_iter => ( is => 'rw' );

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
	defined (my $line= $self->_line_iter->next) or return undef;
	redo if ord $line == _COMMENT || !length $line;
	return $self->_parse_line($line);
}
sub _parse_line {
	my ($self, $line)= @_;
	my $initial_ts= $self->{_cur_ts};
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
	if (--$self->{_index_counter} <= 0) {
		# but wait, not if the timestamp didn't move since the previous record
		if ($initial_ts < $self->{_cur_ts}) {
			my $index= $self->_index;
			if (@$index >= $self->autoindex_size) {
				# compact index by cutting out every odd element
				$index->[$_]= $index->[$_*2] for 1 .. int($#$index/2);
				$#$index= int(@$index/2)-1;
				$self->autoindex_period($self->autoindex_period * 2);
			}
			push @$index, [ $self->{_cur_ts}, $self->_line_iter->line_addr ];
		}
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
		$self->_line_iter->seek($index->[$min][1]) or croak "seek: $!";
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
	$self->{_cur_ts}= 0;
	$self->_line_iter->_line_addr_cache->clear;
	defined(my $last_line= $self->_line_iter->prev)
		or return undef;
	# TODO: If the file contains index comments, read backward until the
	# first index point.
	$self->{next}= $self->_parse_line($last_line);
}

1;
