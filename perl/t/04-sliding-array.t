use Test2::V0;
use HydraLog::SlidingArray;

subtest initial_put => sub {
	my $buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put($_, $_) for 0..7;
	is( $buf, object {
		call min => 0;
		call max => 7;
		call [ get => 0 ], 0;
		call [ get => 7 ], 7;
	}, 'put 0..7');
	
	$buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put(-$_, -$_) for 0..7;
	is( $buf, object {
		call min => -7;
		call max => 0;
		call [ get => 0 ], 0;
		call [ get => -7 ], -7;
	}, 'put 0..-7' );
};

subtest put_opposite => sub {
	my $buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put($_, $_) for 0..7;
	$buf->put(-1, -1);
	is( $buf, object {
		call min => -1;
		call max =>  6;
		call [ get => -2 ], undef;
		call [ get => -1 ], -1;
		call [ get =>  0 ], 0;
		call [ get =>  6 ], 6;
		call [ get =>  7 ], undef;
	}, 'put 0..7, -1');
	
	$buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put(-$_, -$_) for 0..7;
	$buf->put(1, 1);
	is( $buf, object {
		call min => -6;
		call max =>  1;
		call [ get => -7 ], undef;
		call [ get => -6 ], -6;
		call [ get =>  0 ], 0;
		call [ get =>  1 ], 1;
		call [ get =>  2 ], undef;
	}, 'put 0..-7, 1');
};

subtest put_gaps => sub {
	my $buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put(-7, -7);
	is( $buf, object {
		call min => -7;
		call max => -7;
		call [ get => -7 ], -7;
		call [ get => $_ ], undef
			for -6..0;
	}, 'put(-7)' );
};

subtest full_slide => sub {
	# slide array until position is at 7
	my $buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put($_, $_) for 0..7;
	$buf->slide(7);
	is( $buf, object {
		call min => -7;
		call max =>  0;
		call [ get => -7 ], 0;
		call [ get =>  0 ], 7;
		call [ get =>  1 ], undef;
	}, 'put 0..7 slide(7)' );
	
	$buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put(-$_, -$_) for 0..7;
	$buf->slide(-7);
	is( $buf, object {
		call min =>  0;
		call max =>  7;
		call [ get => -1 ], undef;
		call [ get =>  0 ], -7;
		call [ get =>  1 ], -6;
		call [ get =>  7 ], 0;
		call [ get =>  8 ], undef;
	}, 'put 0..-7 slide(-7)' );
};

subtest slide_beyond_pos => sub {
	my $buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put($_, $_) for 0..7;
	$buf->slide(9);
	is( $buf, object {
		call min => -7;
		call max => -2;
		call [ get => -7 ], 2;
		call [ get => -2 ], 7;
		call [ get => -1 ], undef;
	}, 'put 0..7 slide(7)' );
};

subtest multiput_multiget => sub {
	my $buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put(-3, -3, -2, -1, 0, 1, 2, 3, 4);
	is( [ $buf->get(-4..5) ], [ undef, -3..4, undef ], 'write all 8' );
	# overwrite some, preserve one, and truncate the rest.
	$buf->put(-10, (1)x7);
	is( [ $buf->get(-4, -3, -2) ], [ 1, -3, undef ], 'overwrite partial' );
	# preserve none
	$buf->put(10, 1, 2, 3);
	is( $buf, object {
		call min => 10;
		call max => 12;
		call size => 8;
		call count => 3;
		call [ get => 9 ], undef;
		call [ get => 10 ], 1;
	}, 'write 3 beyond existing' );
};

subtest clear => sub {
	my $buf= HydraLog::SlidingArray->new(size => 8);
	$buf->put(-3, -3, -2, -1, 0, 1, 2, 3, 4);
	$buf->clear(-3, 3);
	is( $buf, object {
		call min => 0;
		call max => 4;
		call count => 5;
	}, 'clear from left' );

	$buf->put(-3, -3, -2, -1, 0, 1, 2, 3, 4);
	$buf->clear(-10, 8);
	is( $buf, object {
		call min => -2;
		call max => 4;
		call count => 7;
	}, 'clear overlap left' );

	$buf->put(-3, -3, -2, -1, 0, 1, 2, 3, 4);
	$buf->clear(3, 2);
	is( $buf, object {
		call min => -3;
		call max => 2;
		call count => 6;
	}, 'clear from right' );
	
	$buf->put(-3, -3, -2, -1, 0, 1, 2, 3, 4);
	$buf->clear(1, 10);
	is( $buf, object {
		call min => -3;
		call max => 0;
		call count => 4;
	}, 'clear overlap right' );

	$buf->put(-3, -3, -2, -1, 0, 1, 2, 3, 4);
	$buf->clear(0, 2);
	is( $buf, object {
		call min => -3;
		call max => 4;
		call count => 8;
		call [ get => -1 ], -1;
		call [ get =>  0 ], undef;
		call [ get =>  1 ], undef;
		call [ get =>  2 ], 2;
	}, 'clear middle' );

	$buf->clear;
	is( $buf, object {
		call min => 0;
		call max => -1;
		call count => 0;
	}, 'clear all' );
};

done_testing;
