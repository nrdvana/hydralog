package HydraLog::MultiReader;
use Moo;
use Carp;
our @CARP_NOT= 'HydraLog::LogReader';

=head1 SYNOPSIS

  $reader1= HydraLog::LogReader->open($filename1);
  $reader2= HydraLog::LogReader->open($filename2);
  $multireader= HydraLog::MultiReader->new_merge($reader1, $reader2);
  while (my $rec= $multireader->next) {
    ...
  }

=head1 DESCRIPTION

This object iterates and seeks multiple log files at once, essentially merging
the files into a single stream of events.  It can efficiently handle many log
files at once.

When using this to 'tail' the contents of live log files being written by a
different process, the final records can end up out of order, unless you pair
this object with a HydraLog daemon event stream.

=head1 CONSTRUCTORS

=head2 new

Standard Moo constructor; pass any of the attributes below as a hashref or
key/value list.

=head2 new_merge

  $reader= HydraLog::MultiReader->new_merge(@sources);

Shortcut for C<< ->new(sources => \@sources) >>.

=cut

sub new_merge {
	my ($class, @sources)= @_;
	for (@sources) {
		$_= HydraLog::LogReader->open($_) unless ref;
	}
	$class->new(sources => \@sources);
}

=head1 ATTRIBUTES

=head2 sources

Arrayref of other readers being merged into this reader.

=cut

has sources => ( is => 'rw', required => 1 );
has _heap   => ( is => 'lazy', clearer => 1 );

=head1 METHODS

=head2 seek

  $reader->seek($unix_epoch);
  $reader->seek($DateTime);

Seek to the event nearest the given time so that the next record returned is
from C<< $T >= $unix_epoch >>.  This might seek beyond the end of all records
in which case C<next> will return C<undef>.

=head2 next

  $log_rec= $reader->next

Return the next record in the sequence.

=head2 peek

  $log_rec= $reader->peek

Return the next record in the sequence, without advancing the iteration.

=cut

sub seek {
	my ($self, $goal)= @_;
	$self->_clear_heap;
	$_->seek($goal) for @{ $self->sources };
	return $self;
}

sub peek {
	my $self= shift;
	my $heap= $self->_heap;
	return undef unless @$heap;
	return $heap->[0][1]->peek;
}

sub next {
	my $self= shift;
	my $heap= $self->_heap;
	return undef unless @$heap;
	my $node= $heap->[0];
	my $src= $node->[1];
	my $rec= $src->next;
	_heap_dequeue($heap, 0);
	if (defined (my $next= $src->peek)) {
		$node->[0]= $next->timestamp;
		_heap_enqueue($heap, $node);
	}
	return $rec;
}

# heap design: [ [ $timestamp1, $source1 ], [ $timestamp2, $source2 ], ... ]
# where elements 1 and 2 are the children of 0, and 3 & 4 are the children of 1, and so on.

sub _build__heap {
	my $self= shift;
	my $heap= [];
	for (@{ $self->sources }) {
		my $next= $_->peek;
		_heap_enqueue($heap, [ $next->timestamp, $_ ]) if defined $next;
	}
	$heap;
}

sub _heap_enqueue {
	my ($heap, $entry)= @_;
	push @$heap, $entry;
	my $cur= $#$heap;
	while ($cur > 0) {
		my $parent= int(($cur-1)/2);
		last if $heap->[$parent][0] <= $heap->[$cur][0];
		@{$heap}[ $cur, $parent ] = @{$heap}[ $parent, $cur ];
		$cur= $parent;
	}
}

sub _heap_dequeue {
	my ($heap, $cur)= @_;
	my $floater= pop @$heap;
	my $lim= @$heap;
	while ($cur*2+1 < $lim) {
		my ($c1, $c2)= ($cur*2+1, $cur*2+2);
		if ($c2 < $lim && $heap->[$c2][0] < $heap->[$c1][0]) {
			last if $floater->[0] <= $heap->[$c2][0]; # floater is smallest
			($cur, $heap->[$cur])= ($c2, $heap->[$c2]); # c2 is smallest
		}
		else {
			last if $floater->[0] < $heap->[$c1][0]; # floater is smallest
			($cur, $heap->[$cur])= ($c1, $heap->[$c1]); # c1 is smallest
		}
	}
	$heap->[$cur]= $floater if $cur < $lim;
}

1;
