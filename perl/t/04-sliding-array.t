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

done_testing;
