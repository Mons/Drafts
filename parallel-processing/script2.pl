#!/usr/bin/env perl

use 5.010;
use strict;
use Data::Dumper;
$Data::Dumper::Useqq = 1;

my $par = 2;
my $timeout = 0.001;

use constant MAXBUF => 100;
use constant TIMEOUT => 0.01;

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
use Time::HiRes 'time', 'sleep';

my $eof;
my @in;
my $bin = 0;
my $lin = 0;
my $start = time;

sub read_in () {
    my $buflines = 0;
    while (<>) {
        push @in, $_;
        $bin += length $_;
        $lin ++;
        if ($lin % 10000 == 0) {
            printf "%0.4fM/s; %0.4fL/s\n",$bin/1024/1024/(time-$start),$lin/(time-$start);
        }
        return if ++$buflines > MAXBUF;
    }
    $eof = 1;
}

my %fds;
my ($rvec,$wvec,$evec);
for my $pid (@CH) {
    $fds{fileno( $pipes{$pid}{r} )} = $pipes{$pid}{r};
    $fds{fileno( $pipes{$pid}{w} )} = $pipes{$pid}{w};
    vec($rvec, fileno( $pipes{$pid}{r} ), 1) = 1;
    vec($wvec, fileno( $pipes{$pid}{w} ), 1) = 1;
}

my ($out,$in,$i);

    SELECT:
    while () {
        my ($r,$w,$e) = ($rvec,$wvec,'');
        my $n = select($r,$w,$e,TIMEOUT);
        if ($n) {
            for my $wfd (0..8*length $w) {
                if (vec($w,$wfd,1)) {
                    if ($i > $#in) {
                        if (!$eof) {
                            @in = ();
                            $i = 0;
                            read_in();
                        }
                        else {
                            close $fds{$wfd};
                            warn "close $wfd";
                            vec($wvec,$wfd,1) = 0;
                            next;
                        }
                    }

                    ++$out;
                    syswrite $fds{$wfd}, $in[$i++];
                }
            }
            for my $rfd (0..8*length $r) {
                if (vec($r,$rfd,1)) {
                    my $line = readline( $fds{$rfd} );
                    if (defined $line) {
                        ++$in;
                        
                        #warn "recv from $rfd: $line";
                        # process out line;
                        # for ex: save it to file
                        
                    }
                    else {
                        warn "close $rfd";
                        close $fds{$rfd};
                        vec($rvec,$rfd,1) = 0;
                    }
                }
            }
        }
        else {
            #warn "Select timeout $eof, $out, $in";
            if ($eof and $out == $in) {
                printf "%0.4fM/s; %0.4fL/s\n",$bin/1024/1024/(time-$start),$lin/(time-$start);
                last;
            }
        }
    }
    
    exit;

CHILD:
    warn "$$: i'm child";
    while (<$R>) {
    
        # any synchronous code
        
        syswrite $W,"got $_" or warn "$$: $!"; # nonbuffered io required here
    }
    close $R;
    close $W;
    exit(0);
