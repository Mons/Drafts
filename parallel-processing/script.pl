#!/usr/bin/env perl

use 5.010;
use strict;
use Data::Dumper;
$Data::Dumper::Useqq = 1;

my $par = 2;
my $timeout = 0.001;

use constant RBUF => 1024*1024;

our @CH;
our %pipes;
our ( $R,$W ); # for child
END {
    kill TERM => $_ for @CH;
    waitpid $_,0 for @CH;
}

for (1..$par) {
    pipe (my $rin,my $win) or die "pipe1: $!";
    pipe (my $rout,my $wout) or die "pipe2: $!";
    defined(my $pid = fork()) or die "fork failed: $!";
    if ($pid) {
        # parent
        close $rin;
        close $wout;
        push @CH, $pid;
        $pipes{$pid} = {
            w => $win,
            r => $rout,
        };
    }
    else {
        close $win;
        close $rout;
        $R = $rin;
        $W = $wout;
        %pipes = ();
        @CH = ();
        goto CHILD;
    }
}

warn "$$: i'm master, working with @CH";

use AnyEvent;
use Time::HiRes 'time';
require EV;
require AnyEvent::Handle;

my %rw;
my %ww;
my %ready;
my $bytes_in = 0;
my $lines = 0;
my $start = time;

{ # read stdin
    my $rbuf = '';
    my $in;
    my $closed = 0;
    
    sub write_children($) {
        my $buf = shift;
        return 0 unless %ready;
        my ($pid) = keys %ready;
        delete $ready{$pid};
        if (defined $$buf) {
            $lines++;
            my $w = syswrite($pipes{$pid}{w}, $$buf);
            if ($w == length $$buf) {
                # ok
            }
            elsif (defined $w) {
                my $wbuf = substr($$buf,$w);
                $ww{$pid} = AE::io $pipes{$pid}{w}, 1, sub {
                    warn "$$: $pid writable";
                    my $w = syswrite($pipes{$pid}{w}, $wbuf);
                    if ($w == length $wbuf) {
                        delete $ww{$pid};
                    }
                    elsif ( defined $w ) {
                        $wbuf = substr($wbuf,$w);
                    }
                    else {
                        warn "write to $pid failed: $!";
                        delete $ww{$pid};
                    }
                };
            }
            else {
                warn "write to $pid failed: $!";
            }
            return 1;
        } else {
            close $pipes{$pid}{w};
            delete $pipes{$pid}{w};
            return 1;
        }
    }
    
    sub read_in () {
        while ($rbuf =~ m{\G([^\n]*\n)}gcxso) {
            unless(write_children(\$1)) {
                pos($rbuf) = pos($rbuf) - length $1;
                $rbuf = substr($rbuf,pos $rbuf);
                return;
            }
        }
        $rbuf = substr($rbuf,pos $rbuf);
        if ($closed) {
            if (length $rbuf) {
                return;
            }
            else {
                warn "no more buffers ".Dumper \%pipes;
                1 while (write_children(\(undef)));
            }
            return;
        }
        
        $in ||= AE::io \*STDIN, 0, sub {
            my $r = sysread STDIN, $rbuf, 1024*1024, length $rbuf;
            $bytes_in += $r;
            warn sprintf "$$: ".length($rbuf). " %0.4fM/s %0.4fL/s\n", $bytes_in/1024/1024/(time - $start), $lines/(time - $start);
            if ($r) {
                while ($rbuf =~ m{\G([^\n]*\n)}gcxso) {
                    unless(write_children(\$1)) {
                        pos($rbuf) = pos($rbuf) - length $1;
                        undef $in;
                        last;
                    }
                }
                $rbuf = substr($rbuf,pos $rbuf);
            }
            elsif(defined $r) {
                warn "stdin closed";
                undef $in;
                $closed = 1;
                return;
            }
            else {
                return;
            }
        };
    }
    
    read_in();
    
}

for my $pid (@CH) {
    my $rbuf = '';
    
    $ready{$pid} = 1;
    
    $rw{$pid} = AE::io $pipes{$pid}{r}, 0, sub {
        #warn "$$: $pid readable";
        my $r = sysread $pipes{$pid}{r}, $rbuf, RBUF, length $rbuf;
        if ($r) {
            while ($rbuf =~ m{\G([^\n]*\n)}gcxso) {
                # save out from $1
            }
            if (pos $rbuf < length $rbuf) {
                $rbuf = substr($rbuf,pos $rbuf);
                warn "buffer left from $pid ".Dumper($rbuf);
                return;
            } else {
                $ready{$pid} = 1;
                read_in();
                $rbuf = '';
            }
        }
        elsif (defined $r) {
            warn "Child $pid gone: $!";
            delete $rw{$pid};
            delete $ww{$pid};
            delete $ready{$pid};
            delete $pipes{$pid}{r};
            read_in();
        } else {
            warn "$$: $pid not readable: $!";
            return;
        }
    };
}


EV::loop();

exit;

CHILD:
    warn "$$: i'm child";
    while (<$R>) {
        #warn "$$: >> $_";
        # any synchronous code
        syswrite $W,"got $_" or warn "$$: $!"; # nonbuffered io required here
    }
    close $R;
    close $W;
    exit(0);
