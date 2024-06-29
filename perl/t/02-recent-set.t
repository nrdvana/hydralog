use Test2::V0;
use HydraLog::RecentSet;

my $set= HydraLog::RecentSet->new(qw( a b c d e f ));
is( $set,
   object {
      call count => 6;
      call sub { [ shift->list ] } => [ qw( a b c d e f ) ];
   }
);

is( $set->touch(qw( c b a )), 0, 'touch(c b a)' );
is( [ $set->list ], [qw( d e f c b a )], 'reordered' );

is( [ $set->truncate(4) ], [qw( d e )], 'truncate(4)' );
is( [ $set->list ], [qw( f c b a )], 'four left' );

is( [ $set->truncate(0) ], [qw( f c b a )], 'truncate(0)' );
is( [ $set->list ], [], 'empty' );

done_testing;
