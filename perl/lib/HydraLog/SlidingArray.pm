package HydraLog::SlidingArray;
use Moo;
use Carp;

=head1 DESCRIPTION

This is an array where the "current element" is always at position 0, other elements exist at
negative or positive offsets, and it is efficient to "slide" the entire sequence of elements
toward positive or negative space.  It is implemented as a ring buffer of fixed length, so the
size must be a power of 2, and if you push elements farther and farther on one end of the array,
elements from the other end will be silently forgotten.

Example:

  my $buf= HydraLog::SlidingArray->new(size => 8);
  $buf->put(-1, "foo");
  $buf->put( 0, "bar");
  $buf->put( 1, "baz");
  
  $buf->slide(1);

  $buf->get(-3); # undef
  $buf->get(-2); # foo
  $buf->get(-1); # bar
  $buf->get(0);  # baz
  $buf->get(1);  # undef

=cut

has _buf => ( is => 'rw', default => sub {[]} );
has _pos => ( is => 'rw', default => 0 );
has _min => ( is => 'rw', default => 0 );
has _lim => ( is => 'rw', default => 0 );
sub size { scalar @{$_[0]{buf}} }
sub min { $_[0]->_min - $_[0]->_pos }
sub max { $_[0]->_lim - $_[0]->_pos - 1 }
sub count { $_[0]->_lim - $_[0]->_min }

sub BUILD {
	my ($self, $args)= @_;
	my $size= $args->{size} || 1024;
	# round up to a power of 2
	--$size;
	$size |= ($size >> 32);
	$size |= ($size >> 16);
	$size |= ($size >>  8);
	$size |= ($size >>  4);
	$size |= ($size >>  2);
	$size |= ($size >>  1);
	++$size;
	
	$#{$self->_buf}= $size-1;
}

=head1 METHODS

=head2 get

  $val= $sa->get($idx);

Return the value at position C<$idx> of the array.  The array logically extends infinitely
in positive and negative directions, and any index outside the assigned values returns C<undef>.

=cut

sub get {
	my ($self, $idx)= @_;
	my $pos= $self->_pos;
	return undef if $idx >= $self->_lim - $pos || $idx < $self->_min - $pos;
	my $buf= $self->_buf;
	$buf->[ ($idx+$pos) & $#$buf ];
}

=head2 put

  $sa->put($idx, $val);

Store a value at an index of the array.  If the index falls outside of (L</min>..L</max>) the
extents will be adjusted to include this index.  If changing the extents would cause there to
be more than C<< size - 1 >> elements, some (or all) elements are dropped from the other end.

=cut

sub put {
	my ($self, $idx, $val)= @_;
	my $buf= $self->_buf;
	my $pos= $self->_pos;
	$idx += $pos; # idx is now relative to pos, like the min/lim attributes
	# Does this extend the 'min' attribute?
	if ($idx < $self->_min) {
		# idx will become the new min.  Are we keeping any of the min..lim elements?
		my $newlim= $idx + @$buf;
		if ($newlim > $self->_min && $self->_min < $self->_lim) {
			# clear elements from $idx+1 to min-1
			$buf->[ $_ & $#$buf ]= undef for +($idx+1) .. ($self->_min-1);
			# truncate lim
			$self->_lim($newlim) if $self->_lim > $newlim;
		} else {
			$self->_lim($idx+1);
		}
		$self->_min($idx);
	}
	# Does this extend the 'max' attribute?
	elsif ($idx >= $self->_lim) {
		# idx+1 will become the new max.  Are we keeping any of the min..max elements?
		my $newmin= $idx - @$buf + 1;
		if ($newmin < $self->_lim && $self->_min < $self->_lim) {
			# clear elements from $lim to $idx-1
			$buf->[ $_ & $#$buf ]= undef for $self->_lim .. ($idx-1);
			# truncate min
			$self->_min($newmin) if $self->_min < $newmin;
		} else {
			$self->_min($idx);
		}
		$self->_lim($idx+1);
	}
	$buf->[ $idx & $#$buf ]= $val;
}

=head2 slide

  $sa->slide($offset);

Shift the entire array by an offset.  A positive offset moves all elements toward negative
space.  This can be seen as "advancing" through the array elements so that C<< get(0) >>
returns the element that used to be at C<< get($offset) >>.

=cut

sub slide {
	my ($self, $ofs)= @_;
	my $pos= $self->_pos + $ofs;
	$self->_pos($pos);
	my $buf= $self->_buf;
	if ($ofs > 0) {
		# position moves positive, sliding all elements toward negative space
		# max is allowed to be negative.  But both max and min need clamped to most negative idx
		my $newmin= $pos - @$buf + 1;
		$self->_lim($newmin) if $self->_lim < $newmin;
		if ($self->_min < $newmin) {
			# did all elements slide off? then normalize empty list to position 0.
			if ($self->_lim == $newmin) {
				$self->_lim(0);
				$self->_min(0);
			} else {
				$self->_min($newmin);
			}
		}
	} else {
		my $newlim= $pos + @$buf;
		$self->_min($newlim) if $self->_min > $newlim;
		if ($self->_lim > $newlim) {
			# did all elements slide off? then normalize empty list to position 0.
			if ($self->_min == $newlim) {
				$self->_lim(0);
				$self->_min(0);
			} else {
				$self->_lim($newlim);
			}
		}
	}
	# If pos is more than sizeof buf, normalize it, to prevent eventual integer overflow
	# (unlikely on int64 platforms anyway, but...)
	if ($pos > @$buf || $pos < -@$buf) {
		my $diff= ($pos & $#$buf) - $pos;
		$self->_pos($pos + $diff);
		$self->_min($self->_min + $diff);
		$self->_lim($self->_lim + $diff);
	}
}

1;
