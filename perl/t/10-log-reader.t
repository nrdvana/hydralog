use Test2::V0;
use HydraLog::LogReader;

my $y2020= 1577836800;

subtest basic => sub {
	my $reader= HydraLog::LogReader->new(filename => \<<END);
#!hydralog-dump --format=tsv0
# start_epoch=$y2020	ts_scale=1	example=true
# timestamp_step_hex=0	level	message
0	INFO	Testing 1
10	WARN	Testing 2
END

	like( $reader,
		object {
			call 'format' => 'tsv0';
			call start_epoch => $y2020;
			call timestamp_scale => 1;
			call custom_attrs => { example => 'true' };
			call fields => [qw( timestamp_step_hex level message )],
		},
		'reader object'
	);

	like( $reader->next,
		object {
			call timestamp => $y2020;
			call timestamp_iso8601 => '2020-01-01T00:00:00Z';
			call level => 'INFO';
			call message => 'Testing 1';
		},
		'first record'
	);
	like( $reader->next,
		object {
			call timestamp => $y2020+16;
			call timestamp_iso8601 => '2020-01-01T00:00:16Z';
			call level => 'WARN';
			call message => 'Testing 2';
		},
		'second record'
	);
};

subtest seek_no_index => sub {
	my $reader= HydraLog::LogReader->new(autoindex_period => -1, filename => \<<END);
#!hydralog-dump --format=tsv0
# start_epoch=$y2020	ts_scale=16	example=true
# timestamp_step_hex=0	message
0	Msg1
10	Msg2
10	Msg3
8	Msg4
4	Msg5
4	Msg6
END
	is( $reader->seek($y2020+1)->peek->message, 'Msg2', 'start+1' );
	is( $reader->seek(0)->peek->message, 'Msg1', 'negative' );
	is( $reader->seek($y2020+1.5)->peek->message, 'Msg3', 'start+1.5' );
	is( $reader->seek($y2020+2.5)->peek->message, 'Msg4', 'start+2' );
	is( $reader->seek($y2020+3)->peek->message, 'Msg6', 'start+3' );
	is( $reader->seek($y2020+2.75)->peek->message, 'Msg5', 'start+2.75' );
	is( $reader->seek($y2020+.1)->peek->message, 'Msg2', 'start+.1' );
	is( $reader->_index, [ [ 0, 111 ] ], 'index unchanged' );
};

subtest seek_index => sub {
	my $reader= HydraLog::LogReader->new(autoindex_period => 1, filename => \<<END);
#!hydralog-dump --format=tsv0
# start_epoch=$y2020	ts_scale=16	example=true
# timestamp_step_hex=0	message
0	Msg1
10	Msg2
10	Msg3
8	Msg4
4	Msg5
4	Msg6
END
	is( $reader->_index, [ [ 0, 111 ] ], 'initial index' );
	is( $reader->next->message, 'Msg1' );
	is( $reader->_index, [ [ 0, 111 ] ], 'no change to index' );
	is( $reader->next->message, 'Msg2' );
	is( $reader->_index, [ [ 0, 111 ], [ 0x10, 118 ] ], 'indexed Msg2' );
	is( $reader->next->message, 'Msg3' );
	is( $reader->_index, [ [ 0, 111 ], [ 0x10, 118 ], [ 0x20, 126 ] ], 'indexed Msg3' );
	is( $reader->next->message, 'Msg4' );
	is( $reader->seek($y2020)->peek->message, 'Msg1', 'seek 0' );
	is( $reader->seek($y2020+1)->peek->message, 'Msg2', 'seek 1' );
	is( $reader->seek($y2020+2)->peek->message, 'Msg3', 'seek 2' );
	is( $reader->seek($y2020+3)->peek->message, 'Msg6', 'seek 3' );
	is( $reader->seek($y2020+3.1)->peek, undef, 'seek 3.1 = undef' );
	is( $reader->_index, [
			[ 0, 111 ], [ 0x10, 118 ], [ 0x20, 126 ],
			[ 0x28, 134 ], [ 0x2C, 141 ], [ 0x30, 148 ],
		], 'indexed Msg3' );
};

done_testing;
