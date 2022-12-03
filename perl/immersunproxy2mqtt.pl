#!/usr/bin/perl
#
# Copyright (c) 2020 David M Brooke, davidmbrooke@users.sourceforge.net
#
#
use strict;
use warnings;
use DateTime;
use AnyEvent::MQTT;
use AnyEvent::Handle::UDP;	# libanyevent-handle-udp-perl 

# File-scope variables
our $mqtt;
our $logfile;
our $udp_server;
our $udp_client;
our $ob_data;
our $ob_ae_handle;
our $ob_client_addr;

# Subroutine for Outbound Callback
sub ob_rx_callback {
    ( $ob_data, $ob_ae_handle, $ob_client_addr ) = @_;
    my $cv_mqtt;
    my $bytestring;

    # Generate date/timestamp for logfile
    my $datetime = DateTime->now( )->iso8601( ).'Z';

    # The "exporting" value is a signed Short at byte 29 & 30
    my $exporting = unpack( "x28 s<", $ob_data );
    # The "generating" value is a signed Short at byte 53 & 54
    my $generating = unpack( "x52 s<", $ob_data );
    # The "diverting" value is a signed Short at byte 55 & 56
    my $diverting = unpack( "x54 s<", $ob_data );
    # The "consuming" value is calculated from the others
    my $consuming = 0 - ( $exporting - $generating + $diverting );
    my $importing = 0 - $exporting;

    # Convert data to hex for logfile
    $bytestring = unpack( 'H*', $ob_data );
    ##print $logfile "$datetime $bytestring\n";
    ##print "$datetime  Exporting: $exporting Generating: $generating Diverting: $diverting Consuming: $consuming\n";

    # Check for 'odd' datagram format - must be a better test than this but need to investigate further
    if ( $generating != 0 )
    {
        # Publish the data to MQTT
        $cv_mqtt = $mqtt->publish( topic => "raw/immersun/220172/generating/power", message => $generating );
        $cv_mqtt = $mqtt->publish( topic => "raw/immersun/220172/exporting/power", message => $exporting );
        $cv_mqtt = $mqtt->publish( topic => "raw/immersun/220172/importing/power", message => $importing );
        $cv_mqtt = $mqtt->publish( topic => "raw/immersun/220172/diverting/power", message => $diverting );
        $cv_mqtt = $mqtt->publish( topic => "raw/immersun/220172/consuming/power", message => $consuming );
    }

    # Forward the Outbound packet to live.immersun.com
    my $cv = $udp_client->push_send( $ob_data );
}

# Subroutine for Inbound Callback
sub ib_rx_callback {
    my ( $data, $ae_handle, $client_addr ) = @_;

    # Forward the Inbound packet to the immerLINK Bridge
    my $cv = $udp_server->push_send( $data, $ob_client_addr );
}


# Main Program

# Open the logfile
#open( $logfile, ">>", "/home/pi/immersun.log" )
    #or die "Can't open output file: $!";
#$logfile->autoflush( 1 );


# Connect to the MQTT Broker
$mqtt = AnyEvent::MQTT->new( host => 'mqtt',
			     client_id => 'immersun2mqtt',
			     user_name => 'immersun',
			     password => '<secret>' );
die unless defined $mqtt;

# Create the UDP Server
$udp_server = AnyEvent::Handle::UDP->new( bind => ['172.16.40.81', 87],
					  on_recv => \&ob_rx_callback );
die unless defined $udp_server;

# Create the UDP Client
$udp_client = AnyEvent::Handle::UDP->new( connect => ['136.243.233.46', 87],
					  on_recv => \&ib_rx_callback );
die unless defined $udp_client;

# Run the event loop
AnyEvent->condvar->recv;

# Shouldn't ever get here but just in case...
#close( $logfile );

