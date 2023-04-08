package HydraLog;
use Exporter::Extensible -exporter_setup => 1;

# VERSION
# ABSTRACT: The dynamic multi-headed logging system

=head1 SYNOPSIS

  use HydraLog ':all';

  # Create a new pipe into a HydraLog daemon
  my $out= hydra_open("/run/hydralog/heads.sock", \%opts);
  
  # Redirect a standard handle into a pipe to a HydraLog daemon
  hydra_redirect(\*STDERR, "/run/hydralog/heads.sock", \%opts);
  
  # Tell a HydraLog daemon to read from one of our file handles
  hydra_splice(\*STDIN, "/run/hydralog/heads.sock", \%opts);
  
  # Directly write a log file in HydraLog format
  my $logger1= create_log("example.log");
  
  # Append to a log file in HydraLog format
  my $logger2= append_log("example.log");
  
  # Parse a HydraLog formatted log file
  my $reader= open_log("example.log");
  
=head1 DESCRIPTION

HydraLog is a logging system that consumes pipe output from one or more
programs, writes it into one or more log files, and then can run asynchronous
log processors that comsume the data from the log files.  This way the logs
always have a place to go and the services writing the pipe won't get blocked
even if the log processor is having trouble.

HydraLog is intended to fit into the Daemontools philosophy of logging, where
a service process writes simple messages to STDOUT or STDERR which get read
and processed by a separate logging program.  While daemontools recommends one
logger process for each service, HydraLog is designed to be one process that
listens to potentially many daemons.  This can simplify logging by merging
messages from multiple daemons into the same file, and results in less process
table clutter.  HydraLog also uses a timestamped format for log files that
allows them to be written separately and then merged later, on demand.
HydraLog can also enable log processors to run in an event-driven manner,
without relying on inotify.

=head1 EXPORTS

=head2 create_log

  $logger= create_log($file_path, %options);

Shortcut for L<HydraLog::LogWriter/create>.  The file must not already exist.

=head2 append_log

  $logger= append_log($file_path, %options);

Shortcut for L<HydraLog::LogWriter/append>.  This checks for locks by other
processes.

=head2 open_log

  $reader= open_log(@paths_or_readers);

Shortcut for L<HydraLog::LogReader/open> or L<HydraLog::MultiReader/merge>.

=cut

sub create_log :Export {
	require HydraLog::LogWriter;
	HydraLog::LogWriter->create(@_);
}

sub append_log :Export {
	require HydraLog::LogWriter;
	HydraLog::LogWriter->append(@_);
}

sub open_log :Export {
	if (@_ == 1) {
		require HydraLog::LogReader;
		HydraLog::LogReader->open($_[0]);
	} else {
		require HydraLog::MultiReader;
		HydraLog::MultiReader->new(sources => [@_]);
	}
}

1;
