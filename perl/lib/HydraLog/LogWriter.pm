package HydraLog::LogWriter;
use Moo;
use Carp;
use Const::Fast;
use Time::HiRes qw( clock_gettime CLOCK_MONOTONIC CLOCK_REALTIME );
use JSON;
require HydraLog::LogReader;

=head1 SYNOPSIS

  my $writer= HydraLog::LogWriter->create("example.log");
  $writer->warn("MyMessage");
  # writes record data: timestamp=... level=WARN message=MyMessage
  
  $writer= HydraLog::LogWriter->append("example.log");
  $writer->info("More Things");

  my $writer2= HydraLog::LogWriter->create("rotated.log", $writer);
  
=head1 DESCRIPTION

This object writes log entries to a file.  If you create the file, you can specify
the list of fields to include in each record, and other configuration.  If you append
to the file, it uses the existing list of fields.  You can also create a new log
using an old log as a template, for quick and easy log rotation.

The default fields are timestamp, level, and message.  These handle what is readily
available from the Perl Log::Any-like logging API.  You can also include arbitrary
other fields, and include them in a hashref as the final argument to the logging
method.

=head1 CONSTRUCTORS

=head2 new

Standard Moo constructor, takes a hashref of attributes.

=head2 create

  $class->create($filename);
  $class->create($filename, \%attributes);
  $class->create($filename, $LogReaderOrWriter);

Create a new log file at the given path.  You can supply a L<HydraLog::LogReader|LogReader>
or C<LogWriter> instance instead of attributes, for convenience.  Dies if it can't create
the file.  This also places an advisory lock on the file, if the feature is available.

=head2 append

  $class->append($filename);

Open the given log file for appending.  It must exist and be readable, because the writer
parameters will come from it.  If advisory locks are supported on your system, this will
attempt to lock the file as well, and throw an exception if it fails.  (ensuring only one
writer)

=cut

sub create {
	my ($class, $fname)= (shift, shift);
	my %attrs= (
		filename => $fname,
		@_ != 1? @_
		: ref $_[0] eq 'HASH'? %{ $_[0] }
		: ref($_[0])->isa('HydraLog::LogWriter')? $class->_logwriter_ctor_attrs($_[0])
		: ref($_[0])->isa('HydraLog::LogReader')? $class->_logreader_ctor_attrs($_[0])
		: croak("Can't extract attributes from '$_[0]'")
	);
	%attrs= ($class->_logwriter_ctor_attrs($class), %attrs) if ref $class;
	-f $fname and croak "Log file '$fname' already exists";
	open($attrs{fh}, '+>', $fname) or croak "open($fname): $!";
	_file_assert_only_writer($attrs{fh}, $fname);
	$attrs{filename}= $fname;
	$class->new(\%attrs)->_write_header;
}

sub append {
	my ($class, $fname)= @_;
	my $reader= HydraLog::LogReader->open($fname);
	-f $fname or croak(-e $fname? "Not a file: '$fname'" : "No such file '$fname'");
	open(my $fh, '+>>', $fname) or croak "open($fname): $!";
	_file_assert_only_writer($fh, $fname);
	$class->new(fh => $fh, filename => $fname, $class->_logreader_ctor_attrs($reader));
}

sub _logreader_ctor_attrs {
	my ($self, $reader)= @_;
	return (
		format          => $reader->format,
		fields          => $reader->fields,
		field_defauts   => $reader->field_defaults,
		start_epoch     => $reader->start_epoch,
		timestamp_scale => $reader->timestamp_scale,
		custom_attrs    => $reader->custom_attrs,
	);
}

sub _logwriter_ctor_attrs {
	my ($self, $writer)= @_;
	return (
		format          => $writer->format,
		fields          => $writer->fields,
		field_defaults  => $writer->field_defaults,
		start_epoch     => $writer->start_epoch,
		timestamp_scale => $writer->timestamp_scale,
		custom_attrs    => $writer->custom_attrs,
	);
}

sub BUILD {
	my ($self, $attrs)= @_;

	# Only supported format, for now.
	$self->format eq 'tsv0' or croak "Unsupported format '".$self->format."' (only tsv0 is known)";

	const my @fields => $self->fields? @{ $self->fields } : qw( timestamp_step_hex level message );
	my %field_set= map +($_ => 1), @fields;
	# Timestamp and message are mandatory, for now.
	@fields && $fields[0] =~ /^timestamp/ or croak "The first field must be a timestamp";
	$field_set{message} || $field_set{other_fields}
		or croak "Fields must include 'message' or 'other_fields'";
	$self->_set_fields(\@fields);

	# Include space-saving default values
	my %defaults= $self->field_defaults? %{ $self->field_defaults } : ();
	$defaults{timestamp_step_hex}= 0 if !exists $defaults{timestamp_step_hex};
	$defaults{level}= 'INFO' if !exists $defaults{level} && grep $_ eq 'level', @fields;
	$defaults{level}= $self->level_alias->{uc $defaults{level}}
		if defined $self->level_alias->{uc $defaults{level}};
	const my %defaults_const => %defaults;
	$self->_set_field_defaults(\%defaults_const);

	# custom attrs also should be const
	const my %file_meta => $self->file_meta? %{ $self->file_meta } : ();
	$self->_set_file_meta(\%file_meta);
	
	my $monotonic= clock_gettime(CLOCK_MONOTONIC);
	my $epoch= clock_gettime(CLOCK_REALTIME);
	$epoch= int($epoch) if $self->timestamp_scale == 1;
	$self->_set_start_epoch($epoch)
		unless defined $self->start_epoch;
	$self->_set_start_monotonic($monotonic + ($epoch - $self->start_epoch));
	$self->{_prev_t}= 0 if grep $_ eq 'timestamp_step_hex', @fields;
	$self->{_written}= 0;
	if ($self->index_spacing) {
		$self->{_next_index}= $self->index_spacing;
	}
}

sub _write_header {
	my $self= shift;
	my $header= "#!hydralog-dump --format=".$self->format."\n";
	my %attrs= (
		%{$self->file_meta},
		start_epoch => $self->start_epoch,
		timestamp_scale => $self->timestamp_scale
	);
	$header .= '# '.join("\t", map "$_=$attrs{$_}", sort keys %attrs)."\n";
	my ($fields, $def)= ($self->fields, $self->field_defaults);
	$header .= '# '.join("\t", map $_.(exists $def->{$_} && length $def->{$_}? '='.$def->{$_} : ''), @$fields)."\n";
	$self->fh->print($header) or croak "Can't write: $!";
	$self;
}

=head1 ATTRIBUTES

=head2 filename

The path and filename being written.

=head2 file_meta

Arbitrary file-level metadata to include in the header of the file.

=head2 fh

The Perl filehandle being written.

=head2 format

The file format, currently always 'tsv0', but 'jsonline' is likely to be supported soon.

=head2 fields

An arrayref of field names, whose values will be part of every output row.

=head2 field_defaults

A hashref of C<< { $field_name => $default_value } >>.  Using default values can greatly reduce
the size of the file, but prevents you from being able to record null/empty values for that
field.

=head2 start_epoch

The UNIX epoch when the file was originally created.

=head2 start_monotonic

The Monotonic counter value at the time of the L</start_epoch>.

=head2 timestamp_scale

A multiplier applied to the monotonic time before writing it.  To get the original timestanmp,
divide the integer read from the log line by this number.

Example: if timestamp_scale is 10, then a log-line integer of 567 represents 56.7 seconds
of monotonic time since the file was created.

=head2 level_alias

A mapping C<< { $input => $output } >> to apply to log-level names (attribute C<'level'>).
This is primarily used to collapse and compress log-level names.  The default is:

  EMERGENCY => 'EM', EMERG => 'EM',
  ALERT => 'A',
  CRITICAL => 'C', CRIT => 'C',
  ERROR => 'E', ERR => 'E',
  WARNING => 'W', WARN => 'W',
  NOTICE => 'N', NOTE => 'N',
  INFO => 'I',
  DEBUG => 'D',
  TRACE => 'T',

which collapses all the syslog-compatible names (case-insensitive) into very short strings.
Any name not seen here will be written into the log file un-altered (in original case).

=head2 index_spacing

If you want "indexes" added to the log file (for effecient seeking when using differential
timestamps) set this to a nonzero number of bytes, like C<2**16>.  An index comment will be
written to the file each time it crosses a boundary of this number of bytes.

=cut

has filename        => ( is => 'ro', required => 1 );
has file_meta       => ( is => 'rwp' );
has fh              => ( is => 'ro', required => 1 );
has 'format'        => ( is => 'ro', default => 'tsv0' );
has fields          => ( is => 'rwp' );
has field_defaults  => ( is => 'rwp' );
has start_epoch     => ( is => 'rwp' );
has start_monotonic => ( is => 'rwp' );
has timestamp_scale => ( is => 'ro', default => 1 );
has index_spacing   => ( is => 'ro' );
has level_alias     => ( is => 'lazy' );

has _field_encoders => ( is => 'lazy' );

sub _build_level_alias {
	{
		EMERGENCY => 'EM', EMERG => 'EM',
		ALERT => 'A',
		CRITICAL => 'C', CRIT => 'C',
		ERROR => 'E', ERR => 'E',
		WARNING => 'W', WARN => 'W',
		NOTICE => 'N', NOTE => 'N',
		INFO => 'I',
		DEBUG => 'D',
		TRACE => 'T',
	}
}

=head1 METHODS

=head2 Logging Methods

  $writer->info($message);
  $writer->error('some text', 'more text', { pid => $$ });

The following are designed to allow this object to be a drop-in substitute for other popular
Perl logging objects.  All string arguments are concatenated with ' ' and control characters
are replaced with ' ' to form the C<message> field.  If the final argument is a hashref, its
elements are the field values to be written.  The log level is derived from the name of the
method, and the timestamp is added automatically.

=over

=item trace

=item debug

=item info

=item warn

=item error

=item crit

=item alert

=item emerg

=back

=cut

sub trace { shift->_write('trace' => @_) } sub is_trace { 1 }
sub debug { shift->_write('debug' => @_) } sub is_debug { 1 }
sub info  { shift->_write('info'  => @_) } sub is_info  { 1 }
sub warn  { shift->_write('warn'  => @_) } sub is_warn  { 1 }
sub error { shift->_write('error' => @_) } sub is_error { 1 }
sub crit  { shift->_write('crit'  => @_) } sub is_crit  { 1 }
sub alert { shift->_write('alert' => @_) } sub is_alert { 1 }
sub emerg { shift->_write('emerg' => @_) } sub is_emerg { 1 }

sub _write {
	my $self= shift;
	my $level= shift;
	my $fieldhash= @_ >= 1 && ref $_[-1] eq 'HASH'? pop : undef;
	$self->write(
		($fieldhash? %$fieldhash : ()),
		level => $level,
		(@_ > 0? (message => join(' ', @_)) : ())
	);
}

=head2 write

  $writer->write({ level => 'info', message => $text, ... });

For complete control over fields written, this method lets you specify each field as a hashref.
This does not include ability to override the timestamp.

=cut

sub write {
	my $self= shift;
	my $args= { @_ == 1 && ref $_[0] eq 'HASH'? %{$_[0]} : @_ };
	my $rec= '';
	$rec .= $_->($self, $args) . "\t"
		for @{ $self->_field_encoders };
	substr($rec,-1)= "\n";
	# every N bytes of log, emit an index helper
	if (defined $self->{_prev_t} and defined $self->{_next_index} && $self->{_written} > $self->{_next_index}) {
		my $index= "#\tt=".$self->{_prev_t}."\n";
		$self->{_written} += length $index;
		$self->{_next_index}= (int($self->{_written} / $self->index_spacing)+1) * $self->index_spacing;
	}
	$self->fh->print($rec);
	$self->{_written} += length $rec;
}

# This builds an arrayref of coderefs, which are called in sequence to encode
# each of the fields.
sub _build__field_encoders {
	my $self= shift;
	my @enc;
	for my $f (@{ $self->fields }) {
		push @enc, $self->can("_generate_encode_$f")
			? $self->can("_generate_encode_$f")->($self)
			: $self->can("_encode_$f") || $self->_generate_encode($f);
	}
	return \@enc;
}

sub _generate_encode {
	my ($self, $field)= @_;
	my $default= exists $self->field_defaults->{$field}? $self->field_defaults->{$field} : '';
	return sub {
		my ($self, $args)= @_;
		defined (my $v= delete $args->{$field})
			or return '';
		$v= '' if $v eq $default;
		$v =~ s/[\0-\1F]+/ /g;
		return $v;
	};
}

sub _encode_timestamp_step_hex {
	my ($self, $args)= @_;
	my $t= int((clock_gettime(CLOCK_MONOTONIC) - $self->start_monotonic) * $self->timestamp_scale);
	my $diff= $t - $self->{_prev_t};
	$self->{_prev_t}= $t;
	sprintf("%X", $diff);
}

sub _encode_timestamp {
	my ($self, $args)= @_;
	int((clock_gettime(CLOCK_MONOTONIC) - $self->start_monotonic) * $self->timestamp_scale);
}

sub _generate_encode_level {
	my $self= shift;
	my $default= $self->field_defaults->{level};
	my $aliases= $self->level_alias;
	$default= $aliases->{uc $default} if defined $aliases->{uc $default};
	return sub {
		my ($self, $args)= @_;
		defined (my $level= delete $args->{level})
			or return '';
		my $alias;
		$level= $alias if defined ($alias= $aliases->{uc $level});
		$level= '' if $level eq $default;
		$level =~ s/[\0-\1F]+/ /g;
		return $level;
	}
}

sub _generate_encode_other_fields {
	my $self= shift;
	my $json= JSON->new->utf8->canonical;
	return sub {
		my ($self, $args)= @_;
		return keys %$args? $json->encode($args) : '';
	}
}

=head2 close

Close the log file

=cut

sub close {
	my $self= shift;
	$self->fh->close or croak "close: $!";
	delete $self->{fh};
}

our $HAVE_SETLK= eval 'use Fcntl qw/F_WRLCK SEEK_SET F_SETLK/; 1';
sub _file_assert_only_writer {
	my ($fd, $fname)= @_;
	if ($HAVE_SETLK) {
		my $lockstruct= pack('sslll', Fcntl::F_WRLCK, Fcntl::SEEK_SET, 0, 0, $$);
		fcntl($fd, Fcntl::F_SETLK, $lockstruct) or croak "Logfile '$fname' is being written by another process";
	} else {
		...
	}
}

1;
