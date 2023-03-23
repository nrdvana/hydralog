use Test2::V0;
use HydraLog::LogReader;

my $reader= HydraLog::LogReader->new(filename => \<<END);
#!hydralog-dump --format=tsv0
# start_epoch=1577836800	ts_scale=1	example=true
# timestamp_step_hex=0	level	message
0	INFO	Testing 1
10	WARN	Testing 2
END

like( $reader,
	object {
		call 'format' => 'tsv0';
		call start_epoch => 1577836800;
		call timestamp_scale => 1;
		call custom_attrs => { example => 'true' };
		call fields => [qw( timestamp_step_hex level message )],
	},
	'reader object'
);

like( $reader->next,
	object {
		call timestamp => 1577836800;
		call timestamp_iso8601 => '2020-01-01T00:00:00Z';
		call level => 'INFO';
		call message => 'Testing 1';
	},
	'first record'
);
like( $reader->next,
	object {
		call timestamp => 1577836800+16;
		call timestamp_iso8601 => '2020-01-01T00:00:16Z';
		call level => 'WARN';
		call message => 'Testing 2';
	},
	'second record'
);

done_testing;
