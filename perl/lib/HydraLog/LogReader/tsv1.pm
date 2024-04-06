package HydraLog::LogReader::tsv1;
use Moo;
use Carp;
use POSIX 'ceil';
use HydraLog::LogRecord;
use constant COMMENT => ord('#');
use constant ABS_dT  => ord('=');
our @CARP_NOT= qw( HydraLog::LogReader );
use namespace::clean;
with 'HydraLog::LogReader';

has fields          => ( is => 'rwp' );
has field_encoding  => ( is => 'rwp' );
has field_default   => ( is => 'rwp' );
has start_epoch     => ( is => 'rwp' );
has timestamp_scale => ( is => 'rwp', default => 1 );
has autoindex_period=> ( is => 'ro', default => 256 );
has autoindex_size  => ( is => 'rw', default => 256 );

sub BUILD {
   my ($self, $args)= @_;
   
   # Read all comment lines. '#%' are metadata, and '#:' is the list of columns and may only
   # occur once.
   while (defined (my $line= $self->_line_iter->next)) {
      if (ord $line == COMMENT) {
         if ($line =~ /^#% (.*)/) {
            $self->_parse_metadata($1);
         } elsif ($line =~ /^#: (.*)/) {
            $self->_parse_fields($1);
         }
      }
      else {
         $self->_line_iter->prev;
         last;
      }
   }
   defined $self->fields
      or croak "Header of file ".$self->filename." lacks column declaration";
   defined $self->start_epoch
      or croak "Header of file ".$self->filename." lacks metadata 'start_epoch'";
   $self->_cur_ofs(0);
   $self->_index([ [ 0, $self->_line_iter->next_line_addr ] ]);
   $self->_index_counter($self->autoindex_period);
}

sub _parse_metadata {
   my ($self, $str)= @_;
   $self->_set_file_meta({}) unless defined $self->file_meta;
   for (split /\t/, $str) {
      /^([^=]+)(?:=(.*))/ or croak "Can't parse metadata line at '$_'";
      if ($1 eq 'start_epoch') {
         # TODO: parse date
         $self->_set_start_epoch($2);
      } else {
         $self->file_meta->{$1}= $2 // 1;
      }
   }
}

sub _parse_fields {
   my ($self, $str)= @_;
   defined $self->fields
      and croak "Fields were declared more than once in ".$self->filename;
   my (@fields, @encoding, @default, %seen);
   for (split /\t/, $str) {
      my ($name, $enc, $def)= /^([^:=]+) (?: :([^=]+) )? (?: =(.*) )? $/x
         or croak "Invalid field specification: $str";
      if (!@fields) {
         $name eq 'dT' or croak "First column must be 'dT' (not $name) in ".$self->filename;
         if ($enc) {
            $enc =~ /^\*(\d+)$/ or croak "Invalid ENCODING for column 'dT': $enc";
            $self->_set_timestamp_scale($1);
         }
         defined $def and croak "'dT' cannot have a default value";
      }
      croak "Column '$name' occurs twice" if $seen{$name}++;
      push @fields, $name;
      push @encoding, $enc;
      push @default, $def;
   }
   $self->_set_fields(\@fields);
   $self->_set_field_encoding(\@encoding);
   $self->_set_field_default(\@default);
}

has _cur_ofs        => ( is => 'rw' );
has _index          => ( is => 'rw' );
has _index_counter  => ( is => 'rw' );
has _line_iter      => ( is => 'rw' );
has _prev_rec_vals  => ( is => 'rw' );

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

sub _parse_next {
   my $self= shift;
   defined (my $line= $self->_line_iter->next) or return undef;
   redo if ord $line == COMMENT || !length $line;
   return $self->_parse_line($line);
}
sub _parse_line {
   my ($self, $line)= @_;
   my %rec;
   my $prev_rec= $self->_prev_rec_vals;
   my @vals= split /\t/, $line;
   # continuation of previous line.  This means we aren't starting from the beginning of the record.
   return undef unless length $vals[0];
   # Timestamp
   $vals[0]= (ord $vals[0] == ABS_dT)
      ? _base64_to_int(substr($vals[0], 1))
      : $prev_rec->[0] + _base64_to_int($vals[0]);
   # TODO: leap seconds
   $rec{timestamp}= $self->start_epoch + $vals[0];
   # other fields
   for (1 .. $#{$self->fields}) {
      my $val= $vals[$_];
      $val= $self->field_default->[$_] unless defined $val && length $val;
      $val= $self->_prev_record->[$_] unless defined $val;
      $rec{$self->fields->[$_]}= $vals[$_]= $val;
   }
   # TODO: Check for line continuation
   my $l;
   $rec{level}= $l
      if defined $rec{level} && defined ($l= $self->level_alias->{$rec{level}});
   # If it's time for an index entry, add one.  Test was conducted earlier.
   #if (--$self->{_index_counter} <= 0) {
   #   # but wait, not if the timestamp didn't move since the previous record
   #   if ($initial_ts < $self->{_cur_ts}) {
   #      my $index= $self->_index;
   #      if (@$index >= $self->autoindex_size) {
   #         # compact index by cutting out every odd element
   #         $index->[$_]= $index->[$_*2] for 1 .. int($#$index/2);
   #         $#$index= int(@$index/2)-1;
   #         $self->autoindex_period($self->autoindex_period * 2);
   #      }
   #      push @$index, [ $self->{_cur_ts}, $self->_line_iter->line_addr ];
   #   }
   #}
   # Save for reference
   $self->_prev_rec_vals(\@vals);
   HydraLog::LogRecord->new(%rec);
}

my @b64_alphabet= ('0'..'9','A'..'Z','a'..'z','_','-');
my @b64_val; for (0..$#b64_alphabet) { $b64_val[ord $b64_alphabet[$_]]= $_; }
sub _base64_to_int {
   my $accum= 0;
   $accum= ($accum << 6) + $b64_val[$_] for unpack 'c*', $_[0];
   return $accum;
}
sub _int_to_base64 {
   my $val= $_[0];
   return '0' if $val <= 0;
   my $str= '';
   while ($val > 0) { $str .= $b64_alphabet[$val & 0x3F]; $val >>= 6; }
   return scalar reverse $str;
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
