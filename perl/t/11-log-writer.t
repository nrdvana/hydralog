use Test::Mock::Time;
use Test2::V0;
use HydraLog::LogWriter;
use File::Temp;
use Time::HiRes 'sleep';

sub slurp { local $/= undef; my $fh; open $fh, '<', $_[0] and <$fh> or die $!; }

my $t_tmp_dir= File::Temp->newdir;

subtest create_and_append => sub {
	my $logfile= "$t_tmp_dir/create_and_append.log";
	my $writer= HydraLog::LogWriter->create($logfile, { timestamp_scale => 256 });
	sleep 1;
	$writer->debug("debug");
	$writer->info("info");
	sleep 1;
	$writer->error("error");
	$writer->close;

	#use Regexp::Debugger;
	like( slurp($logfile),
		qr/^[#]!hydralog-dump \x20 --format=tsv0 \n
            [#] \x20 start_epoch=[.\d]+ \t timestamp_scale=256 \n
            [#] \x20 timestamp_step_hex=0 \t level=I \t message \n
            100 \t D \t debug \n
                \t   \t info \n
            100 \t E \t error \n
        $/x,
		'output of first writer'
	);

	sleep 10;
	$writer= HydraLog::LogWriter->append($logfile);
	$writer->warn("restarted");
	$writer->info("version whatever");
	sleep .5;
	$writer->info("ready");
	$writer->close;

	like( slurp($logfile),
		qr/^[#]!hydralog-dump \x20 --format=tsv0 \n
			[#] \x20 start_epoch=[.\d]+ \t timestamp_scale=256 \n
			[#] \x20 timestamp_step_hex=0 \t level=I \t message \n
			100  \t D \t debug \n
			     \t   \t info \n
			100  \t E \t error \n
			1000 \t W \t restarted \n
			     \t   \t version whatever \n
			80   \t   \t ready \n
		$/x,
		'output after append'
	);
};

done_testing;
