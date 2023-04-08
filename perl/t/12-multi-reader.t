use Test2::V0;
use HydraLog::MultiReader;
use HydraLog::LogReader;
use File::Temp;
use Time::HiRes 'sleep';

sub slurp { local $/= undef; my $fh; open $fh, '<', $_[0] and <$fh> or die $!; }

my $reader1= HydraLog::LogReader->open(\<<END);
#!hydralog-dump --format=tsv0
# start_epoch=1000	ts_scale=1	example=true
# timestamp_step_hex=0	level	message
	INFO	Testing 1
A	WARN	Testing 2
FF	INFO	Testing 4
1	INFO	Testing 5
4	INFO	Testing 10
END

my $reader2= HydraLog::LogReader->open(\<<END);
#!hydralog-dump --format=tsv0
# start_epoch=1256	ts_scale=1	example=true
# timestamp_step_hex=0	level	message
	INFO	Testing 3
C	INFO	Testing 6
2	INFO	Testing 9
END

my $reader3= HydraLog::LogReader->open(\<<END);
#!hydralog-dump --format=tsv0
# start_epoch=1256	ts_scale=16	example=true
# timestamp_step_hex=0	level	message
C1	INFO	Testing 7
0F	INFO	Testing 8
END


my $merged= HydraLog::MultiReader->new_merge($reader1, $reader2, $reader3);
like( $merged->peek, object { call timestamp => 1000; call message => 'Testing 1'; }, 'peek' );
like( $merged->next, object { call timestamp => 1000; call message => 'Testing 1'; }, 'next' );
for (2..10) {
	like( $merged->peek, object { call message => 'Testing '.$_; }, "Record $_" )
		or do {
			for (@{ $merged->sources }) {
				my $rec= $_->peek;
				note $rec? ($_->peek->timestamp . ' ' . $_->peek->message) : 'undef';
			}
		};
	$merged->next;
}

done_testing;
