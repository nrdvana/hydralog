use Test2::V0;
use HydraLog::StreamLineIter;

subtest read_forward_mem => sub {
	my $it= HydraLog::StreamLineIter->new(buffer => <<END);
Line1
Line2
END
	is( $it->next, "Line1" );
	is( $it->next, "Line2" );
};

subtest read_backward_mem => sub {
	my $it= HydraLog::StreamLineIter->new(buffer => <<END);
Line1
Line2
END
	is( $it->prev, "Line2" );
	is( $it->prev, "Line1" );
};

done_testing;
