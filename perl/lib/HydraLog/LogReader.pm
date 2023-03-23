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

  #!hydralog-dump --format=tsv
  # start_epoch=$dt	ts_scale=1	$name=$value ...
  # Timestamp	Level	Message
  $TIMESTAMP	$LEVEL	$MESSAGE
  $TIMESTAMP	$LEVEL	$MESSAGE

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
		$self->_set_ts_scale(delete $meta{ts_scale} || 1);
		$self->_set_custom_attrs(\%meta);
		my $line2= $self->fh->getline or croak "Can't read header line";
		chomp $line2;
		$self->_set_fields([ split /\t/, substr($line2,2) ]);
		/^\w+$/ or croak "Invalid field name '$_' in header"
			for @{ $self->fields };
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

has filename     => ( is => 'ro' );
has fh           => ( is => 'rwp' );
has 'format'     => ( is => 'rwp' );
has fields       => ( is => 'rwp' );
has start_epoch  => ( is => 'rwp' );
has ts_scale     => ( is => 'rwp' );
has custom_attrs => ( is => 'rwp' );

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
	$rec{timestamp}= hex $rec{timestamp} if $rec{timestamp};
	$rec{_log_reader}= $self;
	bless \%rec, 'HydraLog::LogReader::Record';
}

=head1 LOG RECORDS

The log records are a blessed hashref of the fields of a log record.  Some fields have
official meaning, but others are just ad-hoc custom fields defined by the user.

All fields can be accessed as attributes, including the ad-hoc custom ones via AUTOLOAD.
Reading the C<timestamp> returns the same integer that was read from the file (but
decoded from hex).  There is a virtual field C<epoch> which scales and adds the timestamp
to the C<start_epoch> of the log file.

=cut

package HydraLog::LogReader::Record {
	use strict;
	use warnings;
	use overload '""' => sub { $_[0]->to_string };

	sub timestamp { $_[0]{timestamp} }

	sub timestamp_epoch {
		return undef unless defined(my $ts= $_[0]{timestamp});
		$ts / $_[0]{_log_reader}->ts_scale + $_[0]{_log_reader}->start_epoch;
	}

	sub timestamp_localtime {
		my $epoch= $_[0]->timestamp_epoch or return undef;
		my ($sec, $min, $hour, $mday, $mon, $year)= localtime $epoch;
		return sprintf "%04d-%02d-%02d %02d:%02d:%02d%s",
			$year+1900, $mon+1, $mday, $hour, $min, $sec,
			($epoch =~ /(\.\d+)$/? $1 : '');
	}

	sub timestamp_gmtime {
		my $epoch= $_[0]->timestamp_epoch or return undef;
		my ($sec, $min, $hour, $mday, $mon, $year)= gmtime $epoch;
		return sprintf "%04d-%02d-%02dT%02d:%02d:%02d%sZ",
			$year+1900, $mon+1, $mday, $hour, $min, $sec,
			($epoch =~ /(\.\d+)$/? $1 : '');
	}

	sub level     { $_[0]{level} }
	sub facility  { $_[0]{facility} }
	sub identity  { $_[0]{identity} }
	sub message   { $_[0]{message} }

	sub to_string {
		my $self= shift;
		return join ' ',
			(defined $self->{timestamp}? ( $self->timestamp_localtime ) : ()),
			(defined $self->{log_level}? ( $self->log_level ) : ()),
			(defined $self->{facility}? ( $self->facility ) : ()),
			(defined $self->{identity}? ( $self->identity ) : ()),
			(defined $self->{message}? ( $self->message ) : ());
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
