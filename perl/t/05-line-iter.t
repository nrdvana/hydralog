use Test2::V0;
use HydraLog::StreamLineIter;

my $text= <<END;
Line 1
Line 2
before empty line

following empty line
1
2



END

subtest read_forward_mem => sub {
   my $it= HydraLog::StreamLineIter->new_static(\$text);
   is( $it->next, "Line 1" );
   is( $it->next, "Line 2" );
   is( $it->next, "before empty line" );
   is( $it->next, "" );
   is( $it->next, "following empty line" );
   is( $it->next, "1" );
   is( $it->next, "2" );
   is( $it->next, "" ) for 1..3;
   is( $it->next, undef );
};

subtest read_forward_seekable => sub {
   open my $fh, '<:raw', \$text or die "open: $!";
   my $it= HydraLog::StreamLineIter->new_seekable($fh);
   is( $it->next, "Line 1" );
   is( $it->next, "Line 2" );
};

subtest read_backward_mem => sub {
   my $it= HydraLog::StreamLineIter->new_static(\$text);
   is( $it->prev, "" ) for 1..3;
   is( $it->prev, "2" );
   is( $it->prev, "1" );
};

subtest read_forward_backward => sub {
   my $it= HydraLog::StreamLineIter->new_static(\$text);
   is( $it->next, "Line 1" );
   is( $it->next, "Line 2" );
   is( $it->next, "before empty line" );
   is( $it->next, "" );
   is( $it->next, "following empty line" );
   is( $it->prev, "" );
   is( $it->prev, "before empty line" );
   is( $it->prev, "Line 2" );
   is( $it->next, "before empty line" );
};

done_testing;
