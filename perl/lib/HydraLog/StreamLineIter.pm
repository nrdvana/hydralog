package HydraLog::StreamLineIter;
use Moo;
use Carp;

=head1 SYNOPSIS

  $lines= HydraLog::StreamLineIter->new(fh => $file_handle);
  # or:
  $lines= HydraLog::StreamLineIter->new(buffer => $bytes);

  while (defined ($first_line= $lines->next)) {
    ...
  }
  $last_line= $lines->prev;

=head1 DESCRIPTION

This object provides line-of-text iteration for a file handle, or static buffer.  It can
iterate both forward and backward on seekable streams, or on non-seekable streams it can
iterate backward for as much of the stream was cached.  The entire stream is cached until
the user calls L</discard>.

The module identifies lines based on their stream address.  This makes it easy (and efficient)
to seek around on a line-based file, but it also means this is not appropriate for streams that
might have more bytes than can be described with the platform's integers.

=head1 CONSTRUCTOR

=head2 new

Standard Moo constructor; pass attributes as a hash.  This object requires either a 'fh' or
'buffer' attribute.

=head2 new_static

  $iter= $class->new_static(\$bytes, %attrs);

Creates an iterator over a static buffer.  The buffer can be provided as a scalar or scalar ref.

=head2 new_seekable

  $iter= $class->new_seekable($file_handle, %attrs);

Creates an iterator on a seekable file handle.

=head2 new_stream

  $iter= $class->new_stream($file_handle, %attrs);

Creates an iterator on a non-seekable file handle.  Reverse iteration will only work for as
much of the stream is buffered.

=cut

sub new_static {
	my ($class, $bytes, %attrs)= @_;
	$attrs{buffer}= ref $bytes? $bytes : \$bytes;
	$attrs{eof}= 1;
	$class->new(\%attrs);
}

sub new_seekable {
	my ($class, $fh, %attrs)= @_;
	$attrs{fh}= $fh;
	$attrs{seekable}= 1;
	$class->new(\%attrs);
}

sub new_stream {
	my ($class, $fh, %attrs)= @_;
	$attrs{fh}= $fh;
	$attrs{seekable}= 0;
	$class->new(\%attrs);
}

=head1 ATTRIBUTES

=head2 fh

The file handle being read, or C<undef> if iterating a static buffer

=head2 seekable

Boolean, whether C<fh> is seekable.

=head2 first_line_addr

Byte offset where the first line begins (in case there is a header to ignore)

=head2 buffer_addr

Address of beginning of buffer

=head2 buffer_len

Number of bytes buffered

=head2 line_addr

Address of the I<start> of the most recent line returned from either L</next> or L</prev>.

=cut

has fh                  => ( is => 'ro' );
has seekable            => ( is => 'rwp' );
has first_line_addr     => ( is => 'rwp' );
has buffer_addr         => ( is => 'rwp' );
sub buffer_len {
	my $n= 0;
	$n += length $$_ for @{ $_[0]->_buffers };
	return $n;
}
has line_addr           => ( is => 'rw' );

# The buffer format used by this module is an arrayref of scalar refs, each
# limited to 'buf_size' (unless it was provided by the user in which case it
# is as big as they gave us)
has _buffer_chunk_size  => ( is => 'rw', default => (1<<16) };
has _buffer_addr        => ( is => 'rw' );
has _buffers            => ( is => 'rw', default => sub {[]} );
has _need_seek          => ( is => 'rw', default => 0 );
has _line_addrs         => ( is => 'rw', default => sub{ [] } );
has _line_idx           => ( is => 'rw', default => -1 );

sub BUILD {
	my ($self, $args)= @_;
	defined $self->fh or defined $args->{buffer}
		or croak "Require 'fh' or 'buffer'";
	my $file_addr= defined $self->fh? sysseek($self->fh, 0, 1) : undef;
	my $blen;
	if (defined $args->{buffer}) {
		# this is either the entire contents of the input, or a leading piece of the
		# input to seed the cache.  It is assumed that the file handle is positioned
		# at the end of the data read into this buffer.
		$self->_buffers->[0]= ref $args->{buffer}? $args->{buffer}
			: do { my $x= $args->{buffer}; \$x; };
		# Buffers are allocated in _buffer_chunk_size pieces, so if this one if larger
		# than that, the chunk size needs enlarged, or this needs split into pieces.
		# Enlarging the chunk size is the cheaper operation.
		$blen= length ${$self->_buffers->[0]};
		$self->_buffer_chunk_size(($blen | 0x3FF) & ~0x3FF)
			if $blen > $self->_buffer_chunk_size;
	}
	if (!defined $self->first_line_addr) {
		$self->first_line_addr(
			# not seekable, doesn't matter
			!defined $file_addr? 0
			# Assume file has been read into buffer, and file pointer is at end of buffer
			: defined $args->{buffer} && $file_addr >= $self->buffer_len?
				$file_addr - $self->buffer_len
			# Assume file pointer is at first line
			: $file_addr
		);
	}
	$self->_set_buffer_addr($self->first_line_addr)
		unless defined $self->buffer_addr;
	$self->_set_seekable(defined $file_addr)
		unless defined $self->seekable;
	if ($blen) {
		my ($pos, $buf, $bufaddr, @addrs)= (-1, $self->_buffers->[0], $self->buffer_addr);
		push @addrs, 0 if $bufaddr == $self->first_line_addr;
		while (($pos= index($$buf, "\n", $pos+1)) >= 0) {
			push @addrs, $bufaddr + $pos + 1;
		}
		$self->_line_addrs(\@addrs);
	}
}

=head1 METHODS

=head2 next

  $line= $lineiter->next;

Return a line of text, or undef at the end of the file.  This returns only whole lines; if the
last line available does not end with C<"\n"> it will not be returned.  You can check L</eof>
to see if the actual end of file was reached cleanly.

This updates L</line_addr> if it succeeds.

=head2 prev

  $line= $lineiter->prev;

Return the line before the last line returned, or undef at the start of the file (or if the
stream isn't seekable and you ran out of buffered history).

This updates L</line_addr> if it succeeds.

=cut

sub next {
	my $self= shift;
	while ($self->_line_idx + 1 >= $#{ $self->_line_addrs }) {
		$self->_load_next or return undef;
	}
	my $idx= $self->_line_idx + 1;
	$self->_line_idx($idx);
	return $self->_get_line($idx);
}

sub prev {
	my $self= shift;
	while ($self->_line_idx < 1) {
		$self->_load_prev or return undef;
		# successful load will change _line_idx if it found new lines
	}
	my $idx= $self->_line_idx - 1;
	$self->_line_idx($idx);
	return $self->_get_line($idx);
}

sub _get_line {
	my ($self, $idx)= @_;
	$idx >= 0 && $idx < $#{ $self->_line_addrs }
		or die "BUG: idx $idx out of bounds";
	my $line_addr0= $self->_line_addrs->[$idx];
	my $line_addr1= $self->_line_addrs->[$idx+1] - 1;
	my $buf_ofs0= $line_addr0 - $self->buffer_addr;
	my $buf_ofs1= $line_addr1 - $self->buffer_addr;
	$buf_ofs0 >= 0 && $buf_ofs1 < @{$self->_buffers}
		or die "BUG: buf_ofs $buf_ofs out of bounds";
	my $mod= $self->_buffer_chunk_size;
	my $buf_idx0= int($buf_ofs0 / $mod);
	$buf_ofs0 -= $buf_idx0 * $mod;
	my $buf_idx1= int($buf_ofs1 / $mod);
	$buf_ofs1 -= $buf_idx1 * $mod;
	$self->line_addr($line_addr0);
	return substr(${$self->_buffers->[$buf_idx0]}, $buf_ofs0, $buf_ofs1-$buf_ofs0)
		if $buf_idx0 == $buf_idx1;
	# The line is split across one or more buffers
	my $ret= substr(${$self->_buffers->[$buf_idx0]}, $buf_ofs0);
	$ret .= ${$self->_buffers->[$_]} for (($buf_idx0+1)..($buf_idx1-1));
	return $ret .= substr(${$self->_buffers->[$buf_idx1]}, 0, $buf_ofs1);
}

=head2 seek

  $line= $lineiter->seek($file_addr);

Return the line containing the C<$file_addr>.  Calling seek may reset the buffers, unless the
address is within one block of the existing buffers in which case they will grow to include the
address.

If there is a line at this address, it returns the line and updates L</line_addr>.  Else it
returns undef.

=cut

sub seek {
	my ($self, $addr)= @_;
	# Check for beginning of file
	return undef if $addr < $self->first_line_addr;
	my $mod= $self->_buffer_chunk_size;
	my $lines= $self->_line_addrs;
	# Is the address more than one block before the current start, or more than one
	# block after the end of the buffer? (and only if we can seek there)
	if ($self->seekable and (
		$addr < $self->_buffer_addr - $mod
		|| $addr > $self->_buffer_addr + (1+@{$self->_buffers})*$mod)
	) {
		# seek to desired block and then reset buffers
		my $block_addr= $addr - $addr % $mod;
		my $arrived= sysseek($self->fh, $block_addr, 0)
			or return undef;
		$self->_buffer_addr($block_addr);
		my $buf;
		@{ $self->_buffers }= (\$buf);
		$self->_need_seek(1);
		@$lines= ();
		$self->_line_idx(0);
		my $got= sysread($self->fh, $buf, $mod);
		return undef unless defined $got;
		my ($pos, $addrs)= (-1, $self->_line_addrs);
		push @$lines, 0 if $block_addr == $self->first_line_addr;
		while (($pos= index($buf, "\n", $pos+1)) >= 0) {
			push @$lines, $block_addr + $pos + 1;
		}
	}
	# Iterate forward or backward until we've found a line that includes $addr.
	# (line could be longer than one buffer, so this may iterate a few times)
	while (!@$lines || $addr < $lines->[0]) {
		$self->_buffer_grow_reverse or return undef;
	}
	while ($addr > $lines->[-1]) {
		$self->_buffer_grow or return undef;
	}
	# Binary search, in case there are lots of lines.
	my ($min, $max)= (0, $#$lines);
	while ($min < $max) {
		my $mid= ($min+$max+1)>>1;
		if ($addr < $lines->[$mid]) {
			$max= $mid-1;
		} else {
			$min= $mid;
		}
	}
	return undef unless $min == $max && $min < $#$lines;
	$self->_line_idx($min);
	return $self->_get_line($min);
}

=head2 release_before

  $lineiter->release_before($file_address);

Release buffers up to C<$file_address>.  This only releases whole buffer chunks, and has no
effect if C<$file_address> is in the first (or only) chunk.

=head2 release_after

  $lineiter->release_after($file_address);

Release buffers after C<$file_address>.  This only releases whole buffer chunks, and has no
effect if C<$file_address> is in the last (or only) chunk.

=cut

sub release_before {
	my ($self, $addr)= @_;
	my $buf_idx= int(($addr - $self->buffer_addr) / $self->_buffer_chunk_size);
	return 0 unless $buf_idx > 0 && $buf_idx <= $#{ $self->_buffers };

	splice(@{$self->_buffers}, 0, $buf_idx);
	my $new_addr= $self->buffer_addr + $buf_idx * $self->_buffer_chunk_size;
	$self->buffer_addr($new_addr);
	my $lines= $self->_line_addrs;
	my $keep_idx= 0;
	$keep_idx++ while $keep_idx <= $#$lines && $lines->[$keep_idx] < $new_addr;
	if ($keep_idx > 0) {
		splice(@$lines, 0, $keep_idx);
		# adjust the line_idx by how many elements were removed.
		$self->_line_idx($self->_line_idx - $keep_idx);
	}
	return 1;
}

sub release_after {
	my ($self, $addr)= @_;
	# Releasing forward prevents growing the buffers, unless the stream is seekable.
	return 0 unless $self->seekable;

	my $buf_idx= int(($addr - $self->buffer_addr) / $self->_buffer_chunk_size);
	return 0 unless $buf_idx >= 0 && $buf_idx < $#{ $self->_buffers };

	splice(@{$self->_buffers}, $buf_idx+1);
	my $end_addr= $self->buffer_addr + $buf_idx * $self->_buffer_chunk_size
		+ length ${$self->_buffers->[$buf_idx]};
	my $lines= $self->_line_addrs;
	$#$lines-- while $#$lines > 0 && $lines->[-1] > $end_addr;
	# The file position no longer matches the end of the buffers, so seek is needed
	$self->_need_seek(1);
	return 1;
}

sub _load_next {
	my $self= shift;
	my $mod= $self->_buffer_chunk_size;
	# Is there a partial buffer to fill?
	my ($buf_addr, $bufref, $got, $lines);
	if (@{$self->_buffers} && length ${$self->_buffers->[-1]} < $mod) {
		$buf_addr= $self->_buffer_addr + $#{$self->_buffers} * $mod;
		$bufref= $self->_buffers->[-1];
		$lines= $self->_line_addrs->[-1];
	} else {
		$buf_addr= $self->_buffer_addr + @{$self->_buffers} * $mod;
		$bufref= \my $buf;
		$lines= [];
		# The buffer begins a new line if it is the first buffer,
		# or if the last character in the previous buffer was "\n".
		push @$lines, 0 if $buf_addr == $self->first_line_addr
			or @{$self->_line_addrs} && @{$self->_line_addrs->[-1]}
				&& $self->_line_addrs->[-1][-1] == $mod;
	}
	if ($self->_need_seek) {
		my $goal= $buf_addr + length $$bufref;
		my $arrived= sysseek($self->fh, $goal, 0) or croak "seek: $!";
		$arrived == $goal or croak "Seek failed (arrived=$arrived, goal=$goal)";
		$self->_need_seek(0);
	}
	$got= sysread($self->fh, $$bufref, $mod - length($$bufref), length($$bufref));
	if (!defined $got) {
		# temporary errors?
		return 0 if $!{EINTR} || $!{EAGAIN} || $!{EWOULDBLOCK};
		croak "read: $!";
	} elsif ($got == 0) {
		# EOF, which is permanent on non-seekable media, or maybe temporary on seekable ones.
		$self->_set_eof(1);
		$self->_need_seek(1) if $self->seekable;
		return 0;
	} else {
		# Find line-ends in the new section of buffer
		my $pos= length $$bufref - $got - 1;
		while (($pos= index($$bufref, "\n", $pos+1)) >= 0) {
			push @$lines, $pos;
		}
		# If the 
		return 1;
	}
}

sub _load_prev {
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

1;
