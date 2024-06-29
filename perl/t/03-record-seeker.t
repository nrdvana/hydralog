use Test2::V0;
use HydraLog::RecordSeeker;
use File::Temp;

# Test all patterns of access to the cached/uncached buffers of a RecordSeeker.
subtest load_buffer => sub {
   # Test the scenario where all data is already in a buffer, and no additional data can be
   # read (because no file handle)
   subtest "on single buffer" => sub {
      for (0 .. 5) {
         subtest "_load_buffer($_)" => sub {
            my $rs= HydraLog::RecordSeeker->new(buffer => "01234");
            my $node= $rs->_load_buffer($_);
            is( $node,
               $_ == 5? undef : object { call key => 0; call value => \"01234"; },
               '_load_buffer return value'
            );
            is( $rs,
               object {
                  call eof => T;
                  call seekable => F;
                  call _buffer_tree => object {
                     call size => 1;
                     call min => object {
                        call key => 0;
                        call value => \"01234";
                     };
                  };
               },
               "RecordSeeker after call"
            );
         };
      }
   };

   # Test a scenario where the RecordSeeker can't seek and keeps building new blocks from
   # the reads on a pipe.
   subtest "read from nonblocking stream" => sub {
      use Socket;
      socketpair(my $fh1, my $fh2, AF_UNIX, SOCK_STREAM, PF_UNSPEC) or die "socketpair: $!";
      $fh1->blocking(0);
      $fh2->blocking(0);
      $fh2->autoflush(1);
      my $rs= HydraLog::RecordSeeker->new(fh => $fh1, block_size => 32);
      is( $rs,
         object {
            call eof => F;
            call seekable => F;
            call _buffer_tree => object {
               call size => 0;
            };
            call _fh_pos => 0;
         },
         'initial state'
      );
      is( $rs->_load_buffer(0), undef, '_load_buffer(0) returns temporary error' );
      is( $rs->_load_buffer(31), undef, '_load_buffer(31) returns temporary error' );
      ok( $fh2->print("01234"), 'queue 5 chars' );
      is( $rs->_load_buffer(0), object { call key => 0; call value => \"01234"; }, '_load_buffer(0)' );
      is( $rs->_load_buffer(31), undef, '_load_buffer(31) still returns temporary error' );
      ok( $fh2->print("56789ABCDEF0123456789ABCDEF0123456789"), 'queue 37 chars' );
      is( $rs->_load_buffer(31), object { call key => 0; call value => \"0123456789ABCDEF0123456789ABCDEF"; }, '_load_buffer(31)' );
      is( $rs->_buffer_tree, object { call size => 1; }, 'tree has one buffer' );
      is( $rs->_load_buffer(32), object { call key => 32; call value => \"0123456789"; }, '_load_buffer(32)' );
      is( $rs->_buffer_tree, object { call size => 2; }, 'tree has two buffers' );
   };

   # Test a scenario where the RecordSeeker is reading a real file handle which is still
   # being written in paralle.
   subtest "read from growing file" => sub {
      my $fh1= File::Temp->new;
      $fh1->autoflush(1);
      $fh1->print("0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF");
      open(my $fh2, "<", "$fh1") or die "open($fh1): $!";
      my $rs= HydraLog::RecordSeeker->new(fh => $fh2, block_size => 32);
      is( $rs,
         object {
            call eof => F;
            call seekable => T;
            call _buffer_tree => object {
               call size => 0;
            };
            call _fh_pos => 0;
         },
         'initial state'
      );
      is( $rs->_load_buffer(0), object { call key => 0; call value => \"0123456789ABCDEF0123456789ABCDEF"; }, '_load_buffer(0)' );
      is( $rs->_load_buffer(32), object { call key => 32; call value => \"0123456789ABCDEF"; }, '_load_buffer(32)' );
      is( $rs->_load_buffer(50), undef, '_load_buffer(50)' );
      is( $rs,
         object {
            call eof => T;
            call _buffer_tree => object {
               call size => 2;
            };
            call _fh_pos => 48;
         },
         'after reading initial data'
      );
      ok( $fh1->print("0123456789ABCDE"), 'append more to the file' );
      is( $rs->_load_buffer(50), object { call key => 32; call value => \"0123456789ABCDEF0123456789ABCDE"; }, '_load_buffer(50)' );
   };

   # Test a scenario where the user has already read some of the file, and has some
   # of it (but not all) in a buffer, and passes this buffer and file handle to the
   # constructor.  Being seekable, the RecordSeeker determines the actual address
   # of the current position, and assumes the buffer is what came right before that.
   subtest "backward read from seekable" => sub {
      my $fh1= File::Temp->new;
      $fh1->autoflush(1);
      $fh1->print("0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF");
      open(my $fh2, "<", "$fh1") or die "open($fh1): $!";
      sysread($fh2, my $buf1, 10) == 10 or die "sysread != 10: $!";
      sysread($fh2, my $buf2, 30) == 30 or die "sysread != 30: $!";

      my $rs= HydraLog::RecordSeeker->new(fh => $fh2, buffer => \$buf2, block_size => 32);
      is( $rs,
         object {
            call eof => F;
            call seekable => T;
            call _buffer_tree => object {
               call size => 1;
               call min => object { call key => 10; call value => \"ABCDEF0123456789ABCDEF01234567"; };
            };
            call _fh_pos => 40;
         },
         'initial state'
      );
      is( $rs->_load_buffer(0), object { call key => 0; call value => \"0123456789"; }, '_load_buffer(0)' );
      is( $rs,
         object {
            call eof => F;
            call _buffer_tree => object {
               call size => 2;
               call min => object { call key => 0;  call value => \"0123456789"; };
               call max => object { call key => 10; call value => \"ABCDEF0123456789ABCDEF01234567"; };
            };
            call _fh_pos => 10;
         },
         'after reading missing first buffer'
      );
      is( $rs->_load_buffer(32), object { call key => 10; call value => \"ABCDEF0123456789ABCDEF01234567"; }, '_load_buffer(32)' );
      is( $rs->_load_buffer(42), object { call key => 40; call value => \"89ABCDEF"; }, '_load_buffer(42)' );
      is( $rs,
         object {
            call eof => F;
            call _buffer_tree => object {
               call size => 3;
            };
            call _fh_pos => 48;
         },
         'after reading third buffer'
      );
   };
};

done_testing;
