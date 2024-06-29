package HydraLog::RecentSet;
use strict;
use warnings;

# ABSTRACT: Maintain a set of keys with ability to purge least recently used
# VERSION

=head1 SYNOPSIS

  my $set= HydraLog::RecentSet->new('a', 'b', 'c');
  $set->touch('d');                 # 'd' added
  $set->touch('a', 'e');            # 'a' refreshed, 'e' added
  my @removed= $set->truncate(3);   # ('b','c') is returned
  my @remaining= $set->list;        # ('d', 'a', 'e') is returned

=cut

use constant {
   # cache attributes
   NODE_MAP => 0, OLDEST => 1, NEWEST => 2,
   # node attributes
   NODE_KEY => 0, REF_FROM_OLDER => 1, NEWER => 2,
};

# Structure:
# NODE_MAP => {
#   $x => $self->[OLDEST],
#   $y => $self->[OLDEST][NEWER],
#   ...
# OLDEST => [
#   NODE_KEY       => $x,
#   REF_FROM_OLDER => \$self->[OLDEST]
#   NEWER          => [
#     NODE_KEY       => $y,
#     REF_FROM_OLDER => \$self->[OLDEST][NEWER],
#     NEWER          => [ ... ]
# NEWEST => $self->[OLDEST][NEWER][NEWER]...[NEWER],
# }

=head1 CONSTRUCTOR

  $set= HydraLog::RecentSet->new();
  $set= HydraLog::RecentSet->new(@keys);

The constructor takes a list of keys to initially touch.

=cut

sub new {
   my $self= bless [ {}, undef, undef ], shift;
   $self->touch(@_) if @_;
   $self;
}

=head1 ATTRIBUTES

=head2 count

Number of keys in the set

=head2 keys

Returns all keys as a list, in random order.  (hash-iteration, which is faster than L</list>)

=head2 list

  @all_keys= $set->list

Returns all keys as a list, in least-recent to most-recent order.

=cut

sub count { return scalar CORE::keys %{$_[0][NODE_MAP]} }

sub keys { return CORE::keys %{$_[0][NODE_MAP]} }

sub list {
   my $node= $_[0][OLDEST];
   my @list;
   while ($node) {
      push @list, $node->[NODE_KEY];
      $node= $node->[NEWER];
   }
   return @list;
}

=head1 METHODS

=head2 touch

  $newly_added= $set->touch(@keys);

Add or refresh a list of keys.  Returns the number newly added.

=cut

sub touch {
   my $self= shift;
   my $added= 0;
   for my $key (@_) {
      if (my $node= $self->[NODE_MAP]{$key}) {
         unless ($node == $self->[NEWEST]) {
            # remove from linkedlist
            my $newer= $node->[NEWER];
            ${$node->[REF_FROM_OLDER]}= $newer;
            $newer->[REF_FROM_OLDER]= $node->[REF_FROM_OLDER];
            # Make it the newest
            $node->[REF_FROM_OLDER]= \$self->[NEWEST][NEWER];
            ${$node->[REF_FROM_OLDER]}= $node;
            $node->[NEWER]= undef;
            $self->[NEWEST]= $node;
         }
      } else {
         my $ref_from_older= defined $self->[NEWEST]? \$self->[NEWEST][NEWER] : \$self->[OLDEST];
         $node= [ $key, $ref_from_older, undef ];
         $$ref_from_older= $node;
         $self->[NEWEST]= $node;
         $self->[NODE_MAP]{$key}= $node;
         ++$added;
      }
   }
   return $added;
}

=head2 contains

  $found= $set->contains($key);

Returns true if the set contains the key

=cut

sub contains { return defined $_[0][NODE_MAP]{$_[1]} }

=head2 truncate

  @removed= $set->truncate($max_count);

Reduce the set to a maximum count of keys, and return all the oldest keys that got removed.

=cut

sub truncate {
   my ($self, $count)= @_;
   my $cur_count= scalar CORE::keys %{$self->[NODE_MAP]};
   my @keys;
   if ($count < $cur_count) {
      my $node= $self->[OLDEST];
      for ($count .. $cur_count-1) {
         push @keys, $node->[NODE_KEY];
         $node= $node->[NEWER];
      }
      delete @{$self->[NODE_MAP]}{@keys};
      $self->[OLDEST]= $node;
      if ($node) {
         $node->[REF_FROM_OLDER]= \$self->[OLDEST];
      } else {
         $self->[NEWEST]= undef;
      }
   }
   return @keys;
}

1;

=head1 SEE ALSO

=over

=item L<Tie::Cache::LRU::LinkedList>

Same idea, but a less efficient tie interface, and no hook for handling items that got removed.

=back
