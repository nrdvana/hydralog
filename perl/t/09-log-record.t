use Test2::V0;
use HydraLog::LogRecord;

my $y2020= 1577836800;

like( HydraLog::LogRecord->new(timestamp => $y2020, message => 'test'),
   object {
      call timestamp => object { call ymd => '2020-01-01'; call time => '00:00:00'; };
      call timestamp_utc => '2020-01-01T00:00:00Z';
      call timestamp_local => qr/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;
   },
   'record for y2k'
);

like(
   HydraLog::LogRecord->new(
      timestamp => $y2020+.75,
      message   => 'test',
      custom1   => 42,
   ),
   object {
      call [ can => 'timestamp_utc' ], T();
      call timestamp_utc => '2020-01-01T00:00:00.75Z';
      call [ can => 'message' ], T();
      call message       => 'test';
      call [ can => 'level' ], T();
      call level         => 'INFO';
      call [ can => 'custom1' ], T();
      call custom1       => 42;
      call [ can => 'custom2' ], U();
      call custom2       => DNE();
   },
   'record with custom1 field'
);

like(
   HydraLog::LogRecord->new(
      timestamp => $y2020+.75,
      level     => 'WARN',
      custom2   => 12,
   ),
   object {
      call [ can => 'timestamp_utc' ], T();
      call timestamp_utc => '2020-01-01T00:00:00.75Z';
      call [ can => 'message' ], T();
      call message       => U();
      call [ can => 'level' ], T();
      call level         => 'WARN';
      call [ can => 'custom1' ], U();
      call custom1       => DNE();
      call [ can => 'custom2' ], T();
      call custom2       => 12;
   },
   'record with custom2 field'
);   

subtest level_visibility => sub {
   my @tests= (
      [ 'info', 'INFO', 1 ],
      [ 'info', 'trace', 1 ],
      [ 'Info', 'warn', 0 ],
      [ 'WARNING', 'debug', 1 ],
      [ 'Panic', 'ALERT', 1 ],
      [ 'alert', 'PANIC', 0 ],
   );
   for (@tests) {
      my ($rec_lev, $cutoff, $is_vis)= @$_;
      my $rec= HydraLog::LogRecord->new(timestamp => $y2020, level => $rec_lev);
      is( !!$rec->level_visible_at($cutoff), !!$is_vis, "$rec_lev ".($is_vis? "visible":"not visible")." at $cutoff" );
   }
};

done_testing;
