use Test2::V0;
use HydraLog::LogWriter;
use File::Temp;
use Time::HiRes 'sleep';

sub slurp { local $/= undef; my $fh; open $fh, '<', $_[0] and <$fh> or die $!; }

my $t_tmp_dir= File::Temp->newdir;
my $writer= HydraLog::LogWriter->create("$t_tmp_dir/test.log", { timestamp_scale => 100 });
$writer->debug("debug");
$writer->info("info");
$writer->close;

#use Regexp::Debugger;
like( slurp("$t_tmp_dir/test.log"),
 qr/^[#]!hydralog-dump \x20 --format=tsv0 \n
     [#] \x20 start_epoch=[.\d]+ \t timestamp_scale=100 \n
     [#] \x20 timestamp_step_hex=0 \t level=I \t message \n
     [0-9A-F]{0,4} \t D \t debug \n
     [0-9A-F]{0,4} \t   \t info \n
 $/x,
 'output of main writer'
);

$writer= HydraLog::LogWriter->append("$t_tmp_dir/test.log");
$writer->error("bad stuff");
$writer->close;

like( slurp("$t_tmp_dir/test.log"),
 qr/^[#]!hydralog-dump \x20 --format=tsv0 \n
     [#] \x20 start_epoch=[.\d]+ \t timestamp_scale=100 \n
     [#] \x20 timestamp_step_hex=0 \t level=I \t message \n
     [0-9A-F]{0,4} \t D \t debug \n
     [0-9A-F]{0,4} \t   \t info \n
     [0-9A-F]{0,4} \t E \t bad \x20 stuff \n
 $/x,
 'output after append'
);

done_testing;
