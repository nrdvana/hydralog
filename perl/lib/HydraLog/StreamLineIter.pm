package HydraLog::StreamLineIter;
use Moo;
use Carp;
use HydraLog::SlidingArray;

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

=head2 line_addr

Address of the I<start> of the most recent line returned from L</next>, L</prev>, or L</seek>.

=cut

has fh                    => ( is => 'ro' );
has seekable              => ( is => 'rwp' );
has first_line_addr       => ( is => 'rwp' );
has line_addr             => ( is => 'rw' );
has line_addr_cache_size  => ( is => 'rw', default => 1024 );

# The buffer format used by this module is an arrayref of scalar refs, each
# limited to 'buf_size' (unless it was provided by the user in which case it
# is as big as they gave us)
has _buffer_addr          => ( is => 'rw' );
sub _buffer_len {
	my $bufs= $_[0]->_buffers;
	return !@$bufs? 0
		: $_[0]->_buffer_chunk_size * $#$bufs + length ${$bufs->[-1]};
}
has _buffer_chunk_size    => ( is => 'rw', default => (1<<16) );
has _buffers              => ( is => 'rw', default => sub {[]} );
has _file_pos             => ( is => 'rw', default => 0 );
has _line_addr_cache      => ( is => 'rw' );

sub BUILD {
	my ($self, $args)= @_;
	defined $self->fh or defined $args->{buffer}
		or croak "Require 'fh' or 'buffer'";
	my $file_addr= defined $self->fh? sysseek($self->fh, 0, 1)
		: undef;
	$self->_set_seekable(defined $file_addr)
		unless defined $self->seekable;
	if (!$self->fh && defined $args->{buffer}) {
		@{$self->_buffers}= ref $args->{buffer}? $args->{buffer} : \$args->{buffer};
		$self->_buffer_chunk_size(length ${$self->_buffers->[0]});
	}
	my $blen;
	if (!defined $self->first_line_addr) {
		$self->_set_first_line_addr(
			# not seekable, doesn't matter
			!defined $file_addr? 0
			# Assume file pointer is at first line
			: $file_addr
		);
	}
	$self->_buffer_addr(!defined $file_addr? 0
		: $file_addr - $file_addr % $self->_buffer_chunk_size
	);
	$self->_file_pos($file_addr || 0);
	$self->_line_addr_cache(HydraLog::SlidingArray->new(size => 1024));
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
	my $linecache= $self->_line_addr_cache;
	if (!$linecache->count) {
		# begin iteration from first_line_addr
		my $first_nl= $self->_find_next_nl($self->first_line_addr);
		defined $first_nl or return undef;
		$linecache->put(0, $self->first_line_addr, $first_nl+1);
	}
	# linecache is initialized, so will have at least 2 entries,
	# idx (start) and idx+1 (end), but need to add another if idx+2 doesn't exist.
	else {
		unless ($linecache->max > 1) {
			my $next_nl= $self->_find_next_nl($linecache->get(1));
			defined $next_nl or return undef;
			$linecache->put(2, $next_nl + 1);
		}
		$linecache->slide(1);
	}
	return $self->_get_line($linecache->get(0,1));
}

sub prev {
	my $self= shift;
	my $linecache= $self->_line_addr_cache;
	if (!$linecache->count) {
		# begin iteration from end of file, if seekable, or fully loaded in buffer
		my $file_addr;
		if ($self->seekable) {
			$file_addr= sysseek($self->fh, 0, 2) or return undef;
		} elsif (!$self->fh) {
			$file_addr= $self->_buffer_len;
		} else {
			return undef;
		}
		my $last_nl= $self->_find_prev_nl($file_addr-1);
		defined $last_nl or return undef;
		my $prev_nl= $self->_find_prev_nl($last_nl-1);
		defined $prev_nl or return undef;
		$linecache->put(0, $prev_nl + 1, $last_nl + 1);
	}
	# linecache is initialized, so will have at least 2 entries,
	# idx (start) and idx+1 (end), but need to add another if idx-1 doesn't exist.
	else {
		unless ($linecache->min < 0) {
			my $prev_nl= $self->_find_prev_nl($linecache->get(0)-2);
			defined $prev_nl or return undef;
			$linecache->put(-1, $prev_nl + 1);
		}
		$linecache->slide(-1);
	}
	return $self->_get_line($linecache->get(0,1));
}

=head2 seek

  $line= $lineiter->seek($file_addr);

If there is a line at this address, and seeking and loading the relevant blocks succeeds, it
returns the line and updates L</line_addr>.  Else it returns undef.

=cut

sub seek {
	my ($self, $addr)= @_;
	# Check for beginning of file
	return undef if $addr < $self->first_line_addr;
	# can we load it?  (might just use cache)
	$self->_load_addr($addr) or return undef;
	# Is the addr within the known lines?
	my $linecache= $self->_line_addr_cache;
	if ($linecache->count
		&& $linecache->get($linecache->min) <= $addr
		&& $addr < $linecache->get($linecache->max)
	) {
		# Binary search.  Indices must be positive, so shift linecache so min=0
		$linecache->slide($linecache->min);
		my ($min, $max)= (0, $linecache->max);
		while ($min < $max) {
			my $mid= ($min+$max+1)>>1;
			if ($addr < $linecache->get($mid)) {
				$max= $mid-1;
			} else {
				$min= $mid;
			}
		}
		if ($min == $max) {
			$linecache->slide($min);
			return $self->_get_line($linecache->get(0,1));
		}
	}
	# Seek backward and forward to find bounding newline chars
	my $prev_nl= $self->_find_prev_nl($addr-1);
	my $next_nl= $self->_find_next_nl($addr);
	return undef unless defined $prev_nl && defined $next_nl;
	$linecache->clear;
	$linecache->put(0, $prev_nl+1, $next_nl+1);
	return $self->_get_line($linecache->get(0,1));
}	

=head2 release_before

  $lineiter->release_before($file_address);

Release buffers up to C<$file_address>.  This only releases whole buffer chunks, and has no
effect if C<$file_address> is in the first (or only) chunk.  It will also not release the
block containing the current line.

=head2 release_after

  $lineiter->release_after($file_address);

Release buffers after C<$file_address>.  This only releases whole buffer chunks, and has no
effect if C<$file_address> is in the last (or only) chunk.  It will also not release the
block containing the current line.

=cut

#sub release_before {
#	my ($self, $addr)= @_;
#	# don't release the buffer holding the current line
#	$addr= $self->line_addr if defined $self->line_addr && $addr > $self->line_addr;
#	my $buf_idx= int(($addr - $self->buffer_addr) / $self->_buffer_chunk_size);
#	return 0 unless $buf_idx > 0 && $buf_idx <= $#{ $self->_buffers };
#
#	splice(@{$self->_buffers}, 0, $buf_idx);
#	my $new_addr= $self->buffer_addr + $buf_idx * $self->_buffer_chunk_size;
#	$self->_set_buffer_addr($new_addr);
#	my $lines= $self->_line_addrs;
#	my $keep_idx= 0;
#	$keep_idx++ while $keep_idx <= $#$lines && $lines->[$keep_idx] < $new_addr;
#	if ($keep_idx > 0) {
#		splice(@$lines, 0, $keep_idx);
#		# adjust the line_idx by how many elements were removed.
#		$self->_line_idx($self->_line_idx - $keep_idx);
#	}
#	return 1;
#}
#
#sub release_after {
#	my ($self, $addr)= @_;
#	# Releasing forward prevents growing the buffers, unless the stream is seekable.
#	return 0 unless $self->seekable;
#
#	# don't release the buffer holding the current line
#	$addr= $self->line_addr if defined $self->line_addr && $addr < $self->line_addr;
#	my $buf_idx= int(($addr - $self->buffer_addr) / $self->_buffer_chunk_size);
#	return 0 unless $buf_idx >= 0 && $buf_idx < $#{ $self->_buffers };
#
#	splice(@{$self->_buffers}, $buf_idx+1);
#	my $end_addr= $self->buffer_addr + $buf_idx * $self->_buffer_chunk_size
#		+ length ${$self->_buffers->[$buf_idx]};
#	my $lines= $self->_line_addrs;
#	$#$lines-- while $#$lines > 0 && $lines->[-1] > $end_addr;
#	# The file position no longer matches the end of the buffers, so seek is needed
#	$self->_need_seek(1);
#	return 1;
#}

sub _get_line {
	my ($self, $addr0, $addr1)= @_;
	$addr1 -= 2; # this points to the next line.  Change it to point to the final char before newline.
	my $bs= $self->_buffer_chunk_size;
	# Verify that the buffer chunks are loaded for this range
	$self->_load_addr_range($addr0, $addr1)
		if $addr0 < $self->_buffer_addr || $addr1 >= $self->_buffer_addr + $self->_buffer_len;
	my $buf0_idx= int(($addr0 - $self->_buffer_addr) / $bs);
	my $buf1_idx= int(($addr1 - $self->_buffer_addr) / $bs);
	my $buf0_ofs= $addr0 % $bs;
	my $buf1_ofs= $addr1 % $bs;
	$self->line_addr($addr0); # let user know address of last returned line
	# single substr if start and end in same buffer
	return substr(${$self->_buffers->[$buf0_idx]}, $buf0_ofs, $buf1_ofs-$buf0_ofs+1)
		if $buf0_idx == $buf1_idx;
	# The line is split across one or more buffers
	my $ret= substr(${$self->_buffers->[$buf0_idx]}, $buf0_ofs);
	$ret .= ${$self->_buffers->[$_]} for +($buf0_idx+1) .. ($buf1_idx-1);
	return $ret .= substr(${$self->_buffers->[$buf1_idx]}, 0, $buf1_ofs+1);
}

sub _find_next_nl {
	my ($self, $addr)= @_;
	my $bs= $self->_buffer_chunk_size;
	while (1) {
		my $buf_idx= int(($addr - $self->_buffer_addr) / $bs);
		my $pos= $addr % $bs;
		# if this address isn't loaded, load it.
		while ($buf_idx < 0 || $buf_idx >= @{$self->_buffers} || $pos >= length ${$self->_buffers->[$buf_idx]}) {
			$self->_load_addr($addr) or return undef;
			# buffer_addr may have changed
			$buf_idx= int(($addr - $self->_buffer_addr) / $bs);
		}
		# Scan through newly-read bytes
		my $found= index(${$self->_buffers->[$buf_idx]}, "\n", $pos);
		if ($found >= 0) {
			$addr += $found - $pos;
			last;
		} else {
			$addr += length ${$self->_buffers->[$buf_idx]} - $pos;
		}
	}
	return $addr;
}

sub _find_prev_nl {
	my ($self, $addr)= @_;
	my $bs= $self->_buffer_chunk_size;
	while ($addr > $self->first_line_addr) {
		my $buf_idx= int(($addr - $self->_buffer_addr) / $bs);
		my $pos= $addr % $bs;
		# if this address isn't loaded, load it.
		while ($buf_idx < 0 || $buf_idx >= @{$self->_buffers} || $pos >= length ${$self->_buffers->[$buf_idx]}) {
			$self->_load_addr($addr) or return undef;
			# buffer_addr may have changed
			$buf_idx= int(($addr - $self->_buffer_addr) / $bs);
		}
		# Scan through newly-read bytes
		my $found= rindex(${$self->_buffers->[$buf_idx]}, "\n", $pos);
		if ($found >= 0) {
			$addr -= $pos - $found;
			last;
		} else {
			$addr -= $pos + 1;
		}
	}
	return $addr >= $self->first_line_addr? $addr : $self->first_line_addr - 1;
}

sub _load_addr {
	my ($self, $addr)= @_;
	my $bs= $self->_buffer_chunk_size;
	my $buf_idx= int( ($addr - $self->_buffer_addr) / $bs);
	my $buf_ofs= $addr % $bs;
	# already loaded?
	return 1 if $buf_idx >= 0 && $buf_idx <= $#{$self->_buffers}
		&& $buf_ofs < length ${$self->_buffers->[$buf_idx]};
	# Trying to fill an existing partial buffer?
	my $bufref= ($buf_idx < 0 || $buf_idx > $#{$self->_buffers})
		? \(my $buf= '')
		: $self->_buffers->[-1];
	my $seek_addr= $self->_buffer_addr + $buf_idx * $bs + length($$bufref);
	if ($seek_addr != $self->_file_pos) {
		return undef unless $self->seekable;
		my $arrived= sysseek($self->fh, $seek_addr, 0);
		defined $arrived or do { carp "seek: $!"; return undef; };
		$arrived == $seek_addr or do { carp "seek arrived at wrong address"; return undef; };
		$self->_file_pos($seek_addr);
	}
	my $got= sysread($self->fh, $$bufref, $bs-length($$bufref), length($$bufref));
	$self->_file_pos($seek_addr + $got) if $got;
	if (!defined $got) {
		# temporary errors?
		return 0 if $!{EINTR} || $!{EAGAIN} || $!{EWOULDBLOCK};
		croak "read: $!";
	} elsif ($got == 0) {
		# EOF, which is permanent on non-seekable media, or maybe temporary on seekable ones.
		$self->_set_eof(1);
		return 0;
	} elsif ($buf_idx == -1) {
		# Reading into an earlier block.
		# Must read a full block, else we'd have a hole in the buffer
		return undef unless $got == $bs;
		unshift @{$self->_buffers}, $bufref;
		$self->_buffer_addr($self->_buffer_addr - $bs);
	} elsif ($buf_idx == @{$self->_buffers}) {
		push @{$self->_buffers}, $bufref;
	} elsif ($buf_idx < -1 || $buf_idx > @{$self->_buffers}) {
		# reset the cache
		@{$self->{_buffers}}= ( $bufref );
		$self->_buffer_addr($self->_buffer_addr + $buf_idx * $bs);
	}
	return $got;
}

# addr1 is the final character to include, not the address beyond the character
sub _load_addr_range {
	my ($self, $addr0, $addr1)= @_;
	# Ensure that the first block at least exists.  This may change the value of _buffer_addr.
	$self->_load_addr($addr0) or return undef;
	# Now verify that all buffers from 0..(N-1) are fully loaded, and that N is loaded to
	# at least $addr1.
	my $base= $self->_buffer_addr;
	my $bs= $self->_buffer_chunk_size;
	my $buf0_idx= int(($addr0 - $base) / $bs);
	my $buf1_idx= int(($addr1 - $base) / $bs);
	# These must be fully loaded
	for ($buf0_idx .. ($buf1_idx-1)) {
		while (length ${$self->_buffers->[$_]} < $bs) {
			$self->_load_addr($base + $_*$bs + length ${$self->_buffers->[$_]})
				or return undef;
		}
	}
	# This needs loaded to at least length > $addr1
	while (length ${$self->_buffers->[$buf1_idx]} < ($addr1 % $bs)) {
		$self->_load_addr($addr1) or return undef;
	}
	return 1;
}

1;
