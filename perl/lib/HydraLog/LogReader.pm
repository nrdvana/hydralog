package HydraLog::LogReader;
use Moo::Role;
use Carp;
use HydraLog::StreamLineIter;
use namespace::clean;

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

=head2 new_autodetect

  $reader= HydraLog::LogReader->new_autodetect(%options);

Generic constructor; determines the actual class of the decoder for the file you are opening
and then loads the appropriate subclass to process it.

=head2 open

  $reader= HydraLog::LogReader->open($filename, %other_options);

Shortcut for C<< ->new_autodetect(filename => $name) >>.

=cut

sub new_autodetect {
   my ($class, %options)= @_;
   ($class, %options)= ( $class->_find_decoder_for_file($options{filename}, $options{fh}), %options );
   $class->new(%options);
}

sub open {
   my ($class, $filename, %options)= @_;
   $class->new_autodetect(filename => $filename, %options);
}

sub _find_decoder_for_file {
   my ($class, $fname, $fh)= @_;
   unless (defined $fh) {
      defined $fname or Carp::croak("You must define either 'filename' or 'fh' attributes");
      CORE::open($fh, '<', $fname) or Carp::croak("open($fname): $!");
   }
   $fname ||= 'log file';
   my $line_iter= HydraLog::StreamLineIter->new(fh => $fh);
   my $line0= $line_iter->next or Carp::croak("Can't read first line of $fname");
   $line0 =~ /^#!.*?--in-format=(\w+)/ or Carp::croak("Can't parse format from first line of $fname");
   my $format= $1;
   require "HydraLog/LogReader/$format.pm";
   my $pkg= "HydraLog::LogReader::$format";
   $pkg->isa($class) or $pkg->DOES($class) or Carp::croak("Perl module $pkg doesn't look like a $class");
   return $pkg, fh => $fh, _line_iter => $line_iter, format => $format;
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
has file_meta       => ( is => 'rwp' );
has level_alias     => ( is => 'lazy' );

=head1 METHODS

=head2 next

Return the next record from the log file.  The records are lightweight blessed objects that
stringify to a useful human-readable line of text.  See L<HydraLog::LogRecord>.

=head2 peek

Return the next record without advancing the iteration.

=cut

sub next {
   my $self= shift;
   defined $self->{next}? delete $self->{next} : $self->_parse_next;
}

sub peek {
   my $self= shift;
   defined $self->{next}? $self->{next} : ($self->{next}= $self->_parse_next);
}

requires 'seek';
requires '_parse_next';

1;
