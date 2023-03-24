package HydraLog::LogWriter;
use Moo;
use Carp;
use Const::Fast;
use Time::HiRes qw( clock_gettime CLOCK_MONOTONIC CLOCK_REALTIME );
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
	# Timestamp and message are mandatory, for now.
	@fields && $fields[0] eq 'timestamp_step_hex' or croak "The first field must be 'timestamp_step_hex'";
	grep $_ eq 'message', @fields or croak "Fields must include 'message'";
	$self->_set_fields(\@fields);

	# Include space-saving default values
	my %defaults= $self->field_defaults? %{ $self->field_defaults } : ();
	$defaults{timestamp_step_hex}= 0 if !exists $defaults{timestamp_step_hex};
	$defaults{level}= 'INFO' if !exists $defaults{level} && grep $_ eq 'level', @fields;
	# Apply the level alias to the default value, for consistency.
	$defaults{level}= $self->level_alias->{uc $defaults{level}} // $defaults{level} if defined $defaults{level};
	const my %defaults_const => %defaults;
	$self->_set_field_defaults(\%defaults_const);

	# custom attrs also should be const
	const my %custom_attrs => $self->custom_attrs? %{ $self->custom_attrs } : ();
	$self->_set_custom_attrs(\%custom_attrs);
	
	my $monotonic= clock_gettime(CLOCK_MONOTONIC);
	my $epoch= clock_gettime(CLOCK_REALTIME);
	$epoch= int($epoch) if $self->timestamp_scale == 1;
	$self->_set_start_epoch($epoch)
		unless defined $self->start_epoch;
	$self->_set_start_monotonic($monotonic + ($epoch - $self->start_epoch));
	$self->{_prev_t}= 0;
}

sub _write_header {
	my $self= shift;
	my $header= "#!hydralog-dump --format=".$self->format."\n";
	my %attrs= (
		%{$self->custom_attrs},
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

=cut

has filename        => ( is => 'ro', required => 1 );
has fh              => ( is => 'ro', required => 1 );
has 'format'        => ( is => 'ro', default => 'tsv0' );
has fields          => ( is => 'rwp' );
has field_defaults  => ( is => 'rwp' );
has start_epoch     => ( is => 'rwp' );
has start_monotonic => ( is => 'rwp' );
has timestamp_scale => ( is => 'ro', default => 1 );
has custom_attrs    => ( is => 'rwp' );
has level_alias     => ( is => 'lazy' );

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
	my $fieldvals= @_ > 1 && ref $_[-1] eq 'HASH'? pop : {};
	my $message= join ' ', @_;
	$message =~ s/[\0-\1F]+/ /g; # remove all control characters except newline
	my $t= int((clock_gettime(CLOCK_MONOTONIC) - $self->start_monotonic) * $self->timestamp_scale);
	my @rec;
	for my $f (@{$self->fields}) {
		if ($f eq 'timestamp_step_hex') {
			# every N bytes log log, emit an index helper
			# ...
			push @rec, sprintf("%X", $t - $self->{_prev_t});
			$self->{_prev_t}= $t;
		} elsif ($f eq 'level') {
			push @rec, $self->level_alias->{uc $level} || $level;
		} elsif ($f eq 'message') {
			push @rec, $message;
		} else {
			push @rec, $fieldvals->{$f} // '';
		}
		$rec[-1]= '' if exists $self->field_defaults->{$f} && $rec[-1] eq $self->field_defaults->{$f};
	}
	$self->fh->print(join("\t", @rec)."\n") or croak "write: $!";
	$self;
}

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
