package HydraLog::RecordSeeker;
use Tree::RB::XS qw( CMP_INT );
use Carp;
use Moo;
use namespace::clean;

has fh                 => ( is => 'ro' );
has eof                => ( is => 'rwp' );
has seekable           => ( is => 'rwp' );
has use_sysread        => ( is => 'rwp' );
has block_size         => ( is => 'ro', default => (1<<20) );

has rec_addr           => ( is => 'rwp' );
has next_rec_addr      => ( is => 'rwp' );

has _buffer_tree       => ( is => 'ro', default => sub { Tree::RB::XS->new(CMP_INT) } );
has _rec_addr_tree     => ( is => 'ro', default => sub { Tree::RB::XS->new(CMP_INT) } );
has _rec_key_tree      => ( is => 'ro', default => sub { Tree::RB::XS->new(CMP_INT) } );
has _fh_pos            => ( is => 'rw' );
has _parse_cache       => ( is => 'rw', default => sub { HydraLog::RecentSet->new } );
has _parse_cache_size  => ( is => 'rw', default => 1024 );

sub BUILD {
   my ($self, $args)= @_;
   defined $self->fh or defined $args->{buffer}
      or croak "Require 'fh' or 'buffer'";
   my $file_addr= defined $self->fh? sysseek($self->fh, 0, 1)
      : undef;
   $self->_set_seekable(defined $file_addr)
      unless defined $self->seekable;
   $self->_set_eof(!0)
      unless defined $self->fh;
   $self->_set_use_sysread($self->fh && fileno($self->fh) >= 0)
      unless defined $self->use_sysread;
   if (defined $args->{buffer}) {
      my $tree= $self->_buffer_tree;
      my @buf_refs= map { ref $_ eq 'SCALAR'? $_ : \$_ }
         ref $args->{buffer} eq 'ARRAY'? @{$args->{buffer}} : ($args->{buffer});
      if (!$self->fh || !defined $file_addr) { # static buffer, or non-seekable stream?
         $file_addr= 0;
         for (@buf_refs) {
            $tree->put($file_addr, $_);
            $file_addr += length $$_;
         }
      } else {
         my $pos= $file_addr;
         for (reverse @buf_refs) {
            $pos -= length $$_;
            $tree->put($pos, $_);
         }
         croak("Buffer provided to ".__PACKAGE__." is larger than current seek position of file")
            if $pos < 0;
      }
   }
   $self->_fh_pos(0+($file_addr || 0));
}

sub next {
   my $self= shift;
   if (!defined $self->next_rec_addr) {
      # Take a guess where we should start looking for the next record.
      $self->_read_next_buffer unless $self->_buffer_tree->size;
      # Search the first buffer for the start of the next record.
      my $addr= $self->_locate_record_from($self->_buffer_tree->min->key);
      if (defined $addr) {
         $self->_set_next_rec_addr($addr);
      } else {
         return undef;
      }
   }
   my ($addr, $len, $key)= $self->_parse_record($self->_buffer_tree->min->key);
   #   $self->_set_rec_addr($addr);
   #   $self->_set_next_rec_addr($addr+$len);
   #}
   #      # No buffers.  Load one and go from there
   #   }
   #   # If there is no buffer, read a block from the stream
   #   $self->_read_next_buffer unless $self->_buffer_tree->size && $self->_buffer_tree->
   #   
   #}
   #if (!defined # begin iteration from first_line_addr
   #   my $first_nl= $self->_find_next_rec($self->first_line_addr || 0);
   #   defined $first_nl or return undef;
   #   
   #}
   # from _next_addr, scan a record.  If it is incomplete, read new buffers until we have it all.
   # put the addr in _rec_addr_tree, and the key in _rec_key_tree.
   # If it is the first record in a range of addresses, mark it as an anchor.
   # If it is not an anchor, mark it as recently used for caching purposes.
   # If too many non-anchors are cached, remove the least recently used.
}

sub seek_addr {
   # Look up _rec_addr_tree.
   # If the addr is less than the first node, we don't have it.
   # If the addr is greater than the last record (after possibly looking up the last record) we don't have it.
   # If the record length reaches the next node, then return that.
   # else begin bisecting the address range looking for the desired record.
   # as new recordss are discovered, mark them and LRU as normal.
}

sub _parse_record {
   my ($self, $addr)= @_;
   my $node= $self->_rec_addr_tree->get_node($addr);
   if ($node) {
      $self->_parse_cache->touch($addr) if $self->_parse_cache->contains($addr);
      return $node;
   }
   # Assume a record starts at $addr.  Find the end of the record by scanning for "\n".
   my $buf_node= $self->_load_buffer($addr)
      or return undef;
   my $end= index ${$buf_node->value}, "\n", ($addr - $buf_node->key);
   while ($ofs < 0) {
      # scan into the next buffer
      $buf_node= $self->_load_buffer($buf_node->key + length ${$buf_node->value})
         or return undef;
      $end= index ${$buf_node->value}, "\n";
   }
   my $end_addr= $buf_node->key + $end;
   $self->_rec_addr_tree->put($addr, $end_addr);
   # If this is the first record to be decoded within a block_size range of the address space,
   # then it is kept as an "anchor" record.  Else it is added to the LRU cached to be discarded.
   my $node= $self->_rec_addr_tree->get($addr);
   my $bs= $self->block_size;
   my ($end_block, $next_block)= ( int($addr / $bs), int($end_addr / $bs) );
   unless ($end_block - $start_block > 2) { # spans an entire block by itself, keep
      my ($prev, $next)= ($node->prev, $node->next);
      if (
         $end_block == $start_block? (
            # if within one block, does it share that with $prev or $next?
               $prev && $start_block == int($prev->addr / $bs)
            or $next && $end_block == int($next->value / $bs)
            )
         ) : (
            # if not within one block, are both blocks shared with a neighbor?
            $prev && $start_block == int($prev->addr / $bs)
            and $next && $end_block == int($next->value / $bs)
         )
      ) {
         # Then this record will be marked temporarily.
         if ($self->_parse_cache->touch($addr)) {
            # Free up old ones
            $self->_rec_addr_tree->delete($_)
               for $self->_parse_cache->truncate($self->_parse_cache_size);
         }
      }
   }
   return $node;
}

sub _load_buffer {
   my ($self, $addr)= @_;
   my $bt= $self->_buffer_tree;
   my $bs= $self->block_size;
   my ($prev, $prev_addr, $prev_len, $next, $next_addr, $next_len, $read_pos, $read_ofs, $read_len, $read_buf);
   if ($prev= $bt->get_node_le($addr)) {
      $prev_addr= $prev->key;
      $prev_len= length ${$prev->value};
      # does that address fall within this buffer? return it.
      return $prev if $prev_addr + $prev_len > $addr;
      # Can we load more data?
      my $fh= $self->fh;
      return undef
         unless $fh && ($self->seekable || !$self->eof);
      # Choose where to start reading.
      if ($prev_len < $bs && $prev_addr + $bs > $addr) {
         # continue filling this buffer
         $read_pos= $prev_addr + $prev_len;
         $read_ofs= $prev_len;
         $read_len= $bs - $prev_len;
         $read_buf= $prev->value;
      }
   }
   if (!defined $read_pos) {
      # round up to block size before this $addr, and start a new block
      $read_pos= $addr - ($addr % $bs);
      $read_len= $bs;
      $read_ofs= 0;
      $read_buf= \(my $buf= '');
      # adjust pos if it overlaps with end of previous buffer
      if ($prev && $read_pos < $prev_addr + $prev_len) {
         my $overlap= ($prev_addr + $prev_len) - $read_pos;
         $read_pos += $overlap;
         $read_len -= $overlap;
      }
   }
   # adjust read_len if it overlaps with the next buffer
   if ($next= $prev? $prev->next : $bt->get_node_gt($addr)) {
      if ($read_pos + $read_len > $next->key) {
         $read_len= $next->key - $read_pos;
      }
   }
   # Seek unless read pos of handle is already there.  This avoids needless syscalls.
   # When not using sysread, need to perform a no-op seek to clear EOF flag on stream.
   if ($read_pos != $self->_fh_pos || ($self->eof && !$self->use_sysread)) {
      return undef unless $self->seekable;
      if ($self->use_sysread) {
         my $arrived= sysseek($self->fh, $read_pos, 0);
         defined $arrived or do { warn "seek: $!"; return undef; };
         $arrived == $read_pos or do { warn "seek arrived at wrong address"; return undef; };
      } else {
         $self->fh->seek($read_pos, 0)
            or do { carp "seek: $!"; return undef; };
      }
      $self->_fh_pos($read_pos+0);
      $self->_set_eof(0);
   }
   my $got= $self->use_sysread
      ? sysread($self->fh, $$read_buf, $read_len, $read_ofs)
      : $self->fh->read($$read_buf, $read_len, $read_ofs);
   if (!defined $got) {
      # temporary errors?
      die "read: $!" unless $!{EINTR} || $!{EAGAIN} || $!{EWOULDBLOCK};
   } elsif ($got == 0) {
      # EOF, which is permanent on non-seekable media, or maybe temporary on seekable ones.
      $self->_set_eof(1);
   } else { # succeeded in loading something...
      $self->_fh_pos($read_pos + $got);
      # If the buffer is newly-allocated, add it to the tree
      if ($read_ofs == 0) {
         $bt->put($read_pos, $read_buf);
         # Did we reach the requested address?
         return $bt->get_node($read_pos) if $read_pos + length $$read_buf > $addr;
      } else {
         # Did we reach the requested address?
         return $prev if $prev_addr + length $$read_buf > $addr;
      }
   }
   return undef;
}

1;
