#!perl
use strict;
use warnings;
use Getopt::Long;
use HydraLog 'open_log';
sub pod2usage { require Pod::Usage; goto &Pod::Usage::pod2usage }

=head1 USAGE

hydralog-dump [OPTIONS] LOGFILE [...]

=head1 OPTIONS

=over

=item (-f) --fields=NAME[,NAME...]

Specify the field names to include in the output.  Build-in fields include:

  timestamp          epoch numbers, possibly fractional
  timestamp_local    YYYY-MM-DD hh:mm:ss[.n]
  timestamp_utc      YYYY-MM-DDThh:mm:ss[.n]Z
  level              ERROR, WARNING, NOTICE, INFO, DEBUG (and others)
  facility           Syslog facility name matching C macro name
  identity           Syslog '$name:' prefix
  message            Message text (single line)

but log writers can include arbitrary other named columns.

=item (-o) --output-format=OUTPUT_TYPE

Specify the format for output.  Current options are 'TSV' and 'JSON'.
Note that TSV is safe without quoting/escaping because messages are not
allowed to contain control characters.

=back

=cut

GetOptions(
	'f|fields=s'        => \(my $opt_fields= 'timestamp_local,level,identity,message'),
	'o|output-format=s' => \(my $opt_out_fmt= 'TSV'),
	'h|help'            => sub { pod2usage(1) },
	'V|version'         => sub { print HydraLog->VERSION."\n"; exit 0; },
) && @ARGV > 0
or pod2usage(2);

my $reader= open_log(@ARGV);

# TODO: verify that each field is contained in at least one input file
my @fields= split ',', $opt_fields;

if (uc($opt_out_fmt) eq 'TSV') {
	while ((my $r= $reader->next)) {
		print join("\t", map $r->$_, @fields)."\n";
	}
} elsif (uc($opt_out_fmt) eq 'JSON') {
	require JSON::MaybeXS;
	my $j= JSON::MaybeXS->new->ascii->canonical;
	while ((my $r= $reader->next)) {
		my %row; @row{@fields}= map $r->$_, @fields;
		print $j->encode(\%row)."\n";
	}
}
